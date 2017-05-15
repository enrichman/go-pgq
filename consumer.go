package pgq

import (
	"database/sql"
	"log"
	"sync"

	// importing postgres driver
	_ "github.com/lib/pq"
)

const (
	registerQuery   = `SELECT register_consumer FROM pgq.register_consumer($1, $2)`
	unregisterQuery = `SELECT unregister_consumer FROM pgq.unregister_consumer($1, $2)`
	nextBatchQuery  = `SELECT next_batch FROM pgq.next_batch($1, $2)`
)

type Consumer struct {
	executor executor
	Queue    string
	Name     string
}

func NewConsumer(conn string, txEnabled bool, queue string, name string) (*Consumer, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Printf("Failed to initialize db connection %s: %v", conn, err)
		return nil, err
	}

	var exec executor
	if txEnabled {
		exec = &txExecutor{db, &sync.Mutex{}, nil}
	} else {
		exec = &simpleExecutor{db}
	}

	return &Consumer{exec, queue, name}, nil
}

func (c *Consumer) Register() (int, error) {
	return c.executor.executeIntQuery(registerQuery, c.Queue, c.Name)
}

func (c *Consumer) Unregister() (int, error) {
	return c.executor.executeIntQuery(unregisterQuery, c.Queue, c.Name)
}

func (c *Consumer) NextBatch() (int, error) {
	return c.executor.executeIntQuery(nextBatchQuery, c.Queue, c.Name)
}
