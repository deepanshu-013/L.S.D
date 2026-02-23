package main

import (
	"context"
	"fmt"
	"highperf-api/internal/auth"
	"highperf-api/internal/cache"
	"highperf-api/internal/clickhouse"
	"highperf-api/internal/config"
	"highperf-api/internal/database"
	"highperf-api/internal/handlers"
	"highperf-api/internal/middleware"
	"highperf-api/internal/pipeline"
	"highperf-api/internal/schema"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	asciiart "github.com/romance-dev/ascii-art"
	_ "github.com/romance-dev/ascii-art/fonts"
)

func main() {
	cfg := config.LoadConfig()
	ctx := context.Background()

	asciiart.NewFigure("L.S.D", "isometric1", true).Print()
	log.Printf("🚀 L.S.D API Server Starting")
	log.Println("═══════════════════════════════════════════════════════════")

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

	// ⭐ ClickHouse Connection Pool
	chPool, err := clickhouse.NewConnectionPool(clickhouse.Config{
		Addr:     cfg.ClickHouseAddr,
		Database: cfg.ClickHouseDB,
		Username: cfg.ClickHouseUser,
		Password: cfg.ClickHousePassword,
	}, 5)

	if err != nil {
		log.Printf("ClickHouse pool creation failed: %v", err)
	}

	var chSearch *clickhouse.SearchRepository
	if chPool != nil && chPool.IsAvailable() {
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
		120*time.Second,
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

	if cdcManager != nil {
		pipelineProcessor.SetCDCTrigger(func(tableName string) error {
			log.Printf("🔄 Pipeline completed for table: %s, triggering CDC sync...", tableName)
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

	// ═══════════════════════════════════════════════════════════
	// 🔐 AUTHENTICATION SETUP
	// ═══════════════════════════════════════════════════════════

	// 1. Get Secret from Environment Variable
	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = "super-secret-dev-key-change-in-production" // Default for dev only
		log.Println("⚠️  WARNING: Using default JWT_SECRET. Set env var JWT_SECRET for production.")
	}

	// 2. Initialize Services
	authService := auth.NewAuthService(jwtSecret)
	authHandler := handlers.NewAuthHandler(pool.Pool, authService)
	authMiddleware := middleware.NewAuthMiddleware(authService)

	// ═══════════════════════════════════════════════════════════
	// 🌐 ROUTER SETUP
	// ═══════════════════════════════════════════════════════════

	mux := http.NewServeMux()

	// ═══════════════════════════════════════════════════════════
	// 🔓 PUBLIC ROUTES (No Token Needed)
	// ═══════════════════════════════════════════════════════════

	mux.HandleFunc("POST /api/auth/login", authHandler.Login)
	mux.HandleFunc("POST /api/auth/register", authHandler.Register) // ⚠️ Disable this in production after creating admin
	mux.HandleFunc("GET /api/health", dynamicHandler.HealthCheck)   // Health check usually public

	// Static Files (Public)
	fs := http.FileServer(http.Dir("./web/"))
	mux.Handle("/web/", http.StripPrefix("/web/", fs))

	// ═══════════════════════════════════════════════════════════
	// 🔒 PROTECTED ROUTES (Token Required)
	// ═══════════════════════════════════════════════════════════

	// We create a sub-mux for protected routes
	protectedMux := http.NewServeMux()

	// Table Endpoints
	protectedMux.HandleFunc("GET /api/tables", dynamicHandler.ListTables)
	protectedMux.HandleFunc("GET /api/tables/{table}/schema", dynamicHandler.GetTableSchema)
	protectedMux.HandleFunc("GET /api/tables/{table}/records", dynamicHandler.GetRecords)
	protectedMux.HandleFunc("GET /api/tables/{table}/records/{pk}", dynamicHandler.GetRecordByPK)
	protectedMux.HandleFunc("GET /api/tables/{table}/stats", dynamicHandler.GetTableStats)
	protectedMux.HandleFunc("GET /api/tables/{table}/search", dynamicHandler.SearchRecords)

	// Search Endpoints (BITMAP PROTECTION)
	protectedMux.HandleFunc("GET /api/search/", dynamicHandler.SearchOptimized)

	// Pipeline Endpoints
	protectedMux.HandleFunc("POST /api/pipeline/start", pipelineHandler.StartJob)
	protectedMux.HandleFunc("GET /api/pipeline/jobs/{job_id}", pipelineHandler.GetJobStatus)
	protectedMux.HandleFunc("GET /api/pipeline/jobs", pipelineHandler.ListJobs)
	protectedMux.HandleFunc("GET /api/pipeline/jobs/{job_id}/stream", pipelineHandler.StreamJobProgress)
	protectedMux.HandleFunc("GET /api/pipeline/jobs/{job_id}/logs", pipelineHandler.GetJobLogs)

	// CDC Status
	protectedMux.HandleFunc("GET /api/cdc/status", dynamicHandler.GetCDCStatus)

	// Mount the protected mux, wrapping it with Auth Middleware
	// We use "/" to catch all requests that didn't match public routes above
	mux.Handle("/", authMiddleware.RequireAuth(protectedMux))

	// ═══════════════════════════════════════════════════════════
	// 🛡️ GLOBAL MIDDLEWARE
	// ═══════════════════════════════════════════════════════════

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

	go func() {
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
