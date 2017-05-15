package pgq

import (
	"database/sql"
	"fmt"
	"sync"
)

type ResolverFunc func(rows *sql.Rows) (interface{}, error)

var intResolverFunc = func(rows *sql.Rows) (interface{}, error) {
	var result int
	for rows.Next() {
		err := rows.Scan(&result)
		if err != nil {
			err := fmt.Errorf("error while scanning row: %v", err)
			rows.Close()
			return 0, err
		}
	}
	return result, nil
}

type executor interface {
	executeIntQuery(query string, args ...interface{}) (int, error)
	executeQuery(resolverFunc ResolverFunc, query string, args ...interface{}) (interface{}, error)
}

type simpleExecutor struct {
	db *sql.DB
}

func (e *simpleExecutor) executeIntQuery(query string, args ...interface{}) (int, error) {
	result, err := e.executeQuery(intResolverFunc, query, args...)
	if err != nil {
		return 0, err
	}
	return result.(int), nil
}

func (e *simpleExecutor) executeQuery(resolverFunc ResolverFunc, query string, args ...interface{}) (interface{}, error) {
	rows, err := e.db.Query(query, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	return resolverFunc(rows)
}

type txExecutor struct {
	db        *sql.DB
	txMutex   *sync.Mutex
	currentTx *sql.Tx
}

func (e *txExecutor) executeIntQuery(query string, args ...interface{}) (int, error) {
	result, err := e.executeQuery(intResolverFunc, query, args...)
	if err != nil {
		return 0, err
	}
	return result.(int), nil
}

func (e *txExecutor) executeQuery(resolverFunc ResolverFunc, query string, args ...interface{}) (interface{}, error) {
	e.txMutex.Lock()
	defer func() {
		e.txMutex.Unlock()
		if e.currentTx != nil {
			e.currentTx = nil
		}
	}()

	tx, err := e.db.Begin()
	if err != nil {
		return nil, err
	}
	e.currentTx = tx

	rows, err := tx.Query(query, args...)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()

	result, err := resolverFunc(rows)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	tx.Commit()
	return result, nil
}
