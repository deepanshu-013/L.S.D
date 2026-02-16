package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"highperf-api/internal/cache"
	"highperf-api/internal/clickhouse"
	"highperf-api/internal/config"
	"highperf-api/internal/database"
	"highperf-api/internal/handlers"
	"highperf-api/internal/middleware"
	"highperf-api/internal/pipeline"
	"highperf-api/internal/schema"
)

func main() {
	cfg := config.LoadConfig()
	ctx := context.Background()

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()
	log.Println("Database connected")

	redisCache := cache.NewRedisCache(
		cfg.RedisAddr,
		cfg.RedisPassword,
		cfg.RedisDB,
		5*time.Minute,
	)

	multiCache := cache.NewMultiLayerCache(redisCache, 30*time.Second)
	log.Println("Multi-layer cache initialized")

	registry := schema.NewSchemaRegistry(pool.Pool)
	if err := registry.LoadSchema(ctx); err != nil {
		log.Fatalf("Failed to load schema: %v", err)
	}
	log.Printf("Schema loaded: %d tables discovered", len(registry.GetAllTables()))

	// ⭐ NEW: Use ConnectionPool instead of single Connection (5× parallel queries)
	chPool, err := clickhouse.NewConnectionPool(clickhouse.Config{
		Addr:     cfg.ClickHouseAddr,
		Database: cfg.ClickHouseDB,
		Username: cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
	}, 5) // 🔥 5 connections for parallel search

	if err != nil {
		log.Printf("ClickHouse pool creation failed: %v", err)
	}

	var chSearch *clickhouse.SearchRepository
	if chPool != nil && chPool.IsAvailable() {
		// ✅ Same constructor, now accepts pool interface
		chSearch = clickhouse.NewSearchRepository(chPool, registry)
		log.Println("✅ ClickHouse search repository initialized with connection pool (5 connections)")
	} else {
		log.Println("⚠️  ClickHouse not available, search will use PostgreSQL only")
	}

	dynamicRepo := database.NewDynamicRepository(pool.Pool, registry)
	dynamicHandler := handlers.NewDynamicHandler(
		dynamicRepo,
		registry,
		multiCache,
		chSearch,
		50, 20,
		120*time.Second, // Increased timeout for large searches
	)

	// Initialize CDC Manager
	var cdcManager *clickhouse.CDCManager
	if chPool != nil && chPool.IsAvailable() && cfg.EnableCDC {
		cdcConfig := clickhouse.CDCConfig{
			BatchSize:       10000,
			SyncInterval:    30 * time.Second,
			ParallelWorkers: 5,
			ChunkSize:       100000,
		}

		cdcManager = clickhouse.NewCDCManager(pool.Pool, chSearch, registry, cdcConfig)
		dynamicHandler.SetCDCManager(cdcManager)
		cdcManager.Start()
		log.Println("🚀 CDC Manager started with auto-discovery")
	}

	// Initialize Pipeline Processor
	pipelineProcessor := pipeline.NewPipelineProcessor(pool.Pool, "./ErrorFiles")

	// ✨ CONNECT PIPELINE TO CDC
	if cdcManager != nil {
		pipelineProcessor.SetCDCTrigger(func(tableName string) error {
			log.Printf("🔄 Pipeline completed for table: %s, triggering CDC sync...", tableName)

			// Trigger immediate sync for the new table
			if err := cdcManager.TriggerTableSync(tableName); err != nil {
				log.Printf("⚠️  CDC sync failed for %s: %v", tableName, err)
				return err
			}

			log.Printf("✅ CDC sync completed for table: %s", tableName)
			return nil
		})
		log.Println("🔗 Pipeline-to-CDC integration enabled")
	}

	pipelineHandler := handlers.NewPipelineHandler(pipelineProcessor)

	mux := http.NewServeMux()

	// ═══════════════════════════════════════════════════════════
	// 🏥 HEALTH & STATUS
	// ═══════════════════════════════════════════════════════════
	mux.HandleFunc("GET /api/health", dynamicHandler.HealthCheck)
	mux.HandleFunc("GET /api/cdc/status", dynamicHandler.GetCDCStatus)

	// ═══════════════════════════════════════════════════════════
	// 📊 TABLES
	// ═══════════════════════════════════════════════════════════
	mux.HandleFunc("GET /api/tables", dynamicHandler.ListTables)
	mux.HandleFunc("GET /api/tables/{table}/schema", dynamicHandler.GetTableSchema)
	mux.HandleFunc("GET /api/tables/{table}/records", dynamicHandler.GetRecords)
	mux.HandleFunc("GET /api/tables/{table}/records/{pk}", dynamicHandler.GetRecordByPK)
	mux.HandleFunc("GET /api/tables/{table}/stats", dynamicHandler.GetTableStats)

	// ═══════════════════════════════════════════════════════════
	// 🔍 SEARCH ENDPOINTS
	// ═══════════════════════════════════════════════════════════

	// Legacy endpoints (backwards compatible)
	// mux.HandleFunc("GET /api/search", dynamicHandler.GlobalSearch)
	mux.HandleFunc("GET /api/tables/{table}/search", dynamicHandler.SearchRecords)

	// ⭐ NEW: Optimized endpoints (5-30× faster)
	mux.HandleFunc("GET /api/search/", dynamicHandler.SearchOptimized)
	// ═══════════════════════════════════════════════════════════
	// 📥 PIPELINE
	// ═══════════════════════════════════════════════════════════
	mux.HandleFunc("POST /api/pipeline/start", pipelineHandler.StartJob)
	mux.HandleFunc("GET /api/pipeline/jobs/{job_id}", pipelineHandler.GetJobStatus)
	mux.HandleFunc("GET /api/pipeline/jobs", pipelineHandler.ListJobs)
	mux.HandleFunc("GET /api/pipeline/jobs/{job_id}/stream", pipelineHandler.StreamJobProgress)
	mux.HandleFunc("GET /api/pipeline/jobs/{job_id}/logs", pipelineHandler.GetJobLogs)

	// ═══════════════════════════════════════════════════════════
	// 🌐 STATIC FILES
	// ═══════════════════════════════════════════════════════════
	fs := http.FileServer(http.Dir("./web/"))
	mux.Handle("/", http.StripPrefix("/", fs))

	handler := middleware.RateLimiter(mux)
	handler = middleware.CORS(handler)
	handler = middleware.Logger(handler)

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.Port),
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	asciiart := ` __        ____         ____      
/\ \      /\  _\      /\  _\    
\ \ \     \ \,\L\_\    \ \ \/\ \  
 \ \ \  __ \/_\__ \     \ \ \ \ \ 
  \ \ \L\ \__/\ \L\ \  __\ \ \_\ \
   \ \____/\_\ \____\/\_\\ \____/
    \/___/\/_/\/_____/\/_/ \/___/ 
                                  
`
	go func() {
		log.Println(asciiart)
		log.Printf("🚀 L.S.D API Server Starting")
		log.Println("═══════════════════════════════════════════════════════════")

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("═══════════════════════════════════════════════════════════")
	log.Println("🛑 Shutting down server gracefully...")
	log.Println("═══════════════════════════════════════════════════════════")

	if cdcManager != nil {
		log.Println("⏸️  Stopping CDC Manager...")
		cdcManager.Stop()
	}

	// ⭐ NEW: Close connection pool
	if chPool != nil {
		log.Println("🔌 Closing ClickHouse connection pool...")
		if err := chPool.Close(); err != nil {
			log.Printf("⚠️  Error closing ClickHouse pool: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("❌ Server forced to shutdown: %v", err)
	}

	log.Println("✅ Server exited properly")
}
