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
	createQueueQuery  = `SELECT pgq.create_queue($1)`
	getQueueInfoQuery = `
	SELECT
		queue_name,
		queue_switch_time,
		EXTRACT(EPOCH FROM queue_rotation_period),
		EXTRACT(EPOCH FROM queue_ticker_max_lag),
		last_tick_id
	FROM
		pgq.get_queue_info($1)`
)

type Client struct {
	executor executor
}

func NewClient(conn string, txEnabled bool) (*Client, error) {
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

	return &Client{exec}, nil
}

func (c *Client) CreateQueue(name string) (int, error) {
	return c.executor.executeIntQuery(createQueueQuery, name)
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

	return &(result.(QueueInfo)), nil
}
