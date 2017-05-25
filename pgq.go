package pgq

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	// importing postgres driver
	_ "github.com/lib/pq"
)

const (
	createQueueQuery        = `SELECT pgq.create_queue($1)`
	registerConsumerQuery   = `SELECT register_consumer FROM pgq.register_consumer($1, $2)`
	unregisterConsumerQuery = `SELECT unregister_consumer FROM pgq.unregister_consumer($1, $2)`
	nextBatchQuery          = `SELECT next_batch FROM pgq.next_batch($1, $2)`
	getQueueInfoQuery       = `
		SELECT
			queue_name,
			queue_switch_time,
			EXTRACT(EPOCH FROM queue_rotation_period),
			EXTRACT(EPOCH FROM queue_ticker_max_lag),
			last_tick_id
		FROM
			pgq.get_queue_info($1)`
)

type ClientOptionSetter func(*Client) error

func WithTxEnabled(enable bool) ClientOptionSetter {
	return func(c *Client) error {
		c.enableTx(enable)
		return nil
	}
}

type Client struct {
	executor executor
}

func NewClient(conn string, opts ...ClientOptionSetter) (*Client, error) {
	db, err := sql.Open("postgres", conn)
	if err != nil {
		log.Printf("Failed to initialize db connection %s: %v", conn, err)
		return nil, err
	}

	client := &Client{&simpleExecutor{db}}
	for _, opt := range opts {
		if err = opt(client); err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (c *Client) enableTx(enable bool) {
	if enable {
		c.executor = &txExecutor{c.executor.DB(), &sync.Mutex{}, nil}
	} else {
		c.executor = &simpleExecutor{c.executor.DB()}
	}
}

func (c *Client) CreateQueue(name string) (int, error) {
	return c.executor.executeIntQuery(createQueueQuery, name)
}

func (c *Client) RegisterConsumer(queue, name string) (int, error) {
	return c.executor.executeIntQuery(registerConsumerQuery, queue, name)
}

func (c *Client) UnregisterConsumer(queue, name string) (int, error) {
	return c.executor.executeIntQuery(unregisterConsumerQuery, queue, name)
}

func (c *Client) NextBatch(queue, name string) (int, error) {
	return c.executor.executeIntQuery(nextBatchQuery, queue, name)
}

/*
queue_name               | string
queue_ntables            |
queue_cur_table          |
queue_rotation_period    |
queue_switch_time        |
queue_external_ticker    |
queue_ticker_paused      |
queue_ticker_max_count   |
queue_ticker_max_lag     |
queue_ticker_idle_period |
ticker_lag               |
ev_per_sec               |
ev_new                   |
last_tick_id             | bigint
*/

type QueueInfo struct {
	QueueName               string
	QueueSwitchTime         time.Time
	QueueRotationPeriodSecs int64
	QueueTickerMaxLagSecs   int64
	LastTickID              int64
}

func (c *Client) GetQueueInfo(name string) (*QueueInfo, error) {
	resolverFunc := func(rows *sql.Rows) (interface{}, error) {
		var result QueueInfo
		for rows.Next() {
			err := rows.Scan(
				&result.QueueName,
				&result.QueueSwitchTime,
				&result.QueueRotationPeriodSecs,
				&result.QueueTickerMaxLagSecs,
				&result.LastTickID,
			)
			if err != nil {
				fmt.Println(err)
				err := fmt.Errorf("error while scanning row: %v", err)
				return nil, err
			}
		}
		return result, nil
	}
	result, err := c.executor.executeQuery(resolverFunc, getQueueInfoQuery, name)
	if err != nil {
		return nil, err
	}

	qi := result.(QueueInfo)
	return &qi, nil
}
