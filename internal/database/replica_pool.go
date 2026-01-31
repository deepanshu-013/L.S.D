package database

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ReplicaPool struct {
	master   *Pool
	replicas []*Pool
	counter  uint64
}

func NewReplicaPool(masterDSN string, replicaDSNs []string) (*ReplicaPool, error) {
	ctx := context.Background()

	master, err := NewPool(ctx, masterDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to master: %w", err)
	}

	log.Printf("Connected to master database")

	var replicas []*Pool
	for i, dsn := range replicaDSNs {
		replica, err := NewPool(ctx, dsn)
		if err != nil {
			log.Printf("Warning: Failed to connect to replica %d: %v", i, err)
			continue
		}
		replicas = append(replicas, replica)
		log.Printf("Connected to replica %d", i)
	}

	if len(replicas) == 0 {
		log.Println("No replicas available, using master for reads")
	}

	return &ReplicaPool{
		master:   master,
		replicas: replicas,
	}, nil
}

func (rp *ReplicaPool) GetMaster() *Pool {
	return rp.master
}

func (rp *ReplicaPool) GetReplica() *Pool {
	if len(rp.replicas) == 0 {
		return rp.master
	}

	idx := atomic.AddUint64(&rp.counter, 1) % uint64(len(rp.replicas))
	return rp.replicas[idx]
}

func (rp *ReplicaPool) GetMasterPool() *pgxpool.Pool {
	return rp.master.Pool
}

func (rp *ReplicaPool) GetReplicaPool() *pgxpool.Pool {
	return rp.GetReplica().Pool
}

func (rp *ReplicaPool) Close() {
	if rp.master != nil {
		rp.master.Close()
	}
	for _, replica := range rp.replicas {
		if replica != nil {
			replica.Close()
		}
	}
}
