package pgq

import (
	"database/sql"
	"fmt"
	"sync"
)

type executor struct {
	db        *sql.DB
	tx        bool
	txMutex   *sync.Mutex
	currentTx *sql.Tx
}

func (e *executor) executeIntQuery(query string, args ...interface{}) (int, error) {
	rows, err := e.executeQuery(query, args...)
	if err != nil {
		e.closeError(rows)
		return 0, err
	}

	var result int
	for rows.Next() {
		err = rows.Scan(&result)
		if err != nil {
			err := fmt.Errorf("error while scanning row: %v", err)
			e.closeError(rows)
			return 0, err
		}
	}
	e.closeSuccess(rows)

	return result, nil
}

func (e *executor) executeQuery(query string, args ...interface{}) (*sql.Rows, error) {
	e.txMutex.Lock()
	defer e.txMutex.Unlock()

	if e.tx {
		fmt.Println("starting tx")

		tx, err := e.db.Begin()
		if err != nil {
			return nil, err
		}
		e.currentTx = tx
		return tx.Query(query, args...)
	}
	return e.db.Query(query, args...)
}

func (e *executor) closeSuccess(rows *sql.Rows) {
	rows.Close()
	if e.tx {
		err := e.currentTx.Commit()
		if err != nil {
			fmt.Println(err)
		}
		e.currentTx = nil
	}
}

func (e *executor) closeError(rows *sql.Rows) {
	rows.Close()
	if e.tx {
		err := e.currentTx.Rollback()
		if err != nil {
			fmt.Println(err)
		}
		e.currentTx = nil
	}
}
