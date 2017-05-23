package pgq

import (
	"testing"

	"errors"

	"sync"

	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

func Test_CreateQueue(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	client := &Client{&simpleExecutor{db}}

	q := mock.ExpectQuery("[" + createQueueQuery + "]").WithArgs("js")
	q.WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))
	res, err := client.CreateQueue("js")
	assert.NotNil(t, res, "missing result")
	assert.Equal(t, 1, res, "unexpected result")
	assert.Nil(t, err, "unexpected error")

	q = mock.ExpectQuery("[" + createQueueQuery + "]").WithArgs("js")
	q.WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(0))
	res, err = client.CreateQueue("js")
	assert.NotNil(t, res, "missing result")
	assert.Equal(t, 0, res, "unexpected result")
	assert.Nil(t, err, "unexpected error")

	q = mock.ExpectQuery("[" + createQueueQuery + "]").WithArgs("js")
	q.WillReturnError(errors.New("db error"))
	res, err = client.CreateQueue("js")
	assert.NotNil(t, err, "missing error")
	assert.Equal(t, 0, res, "unexpected result")

	//	mock.ExpectExec("INSERT INTO product_viewers").WithArgs(2, 3).WillReturnResult(sqlmock.NewResult(1, 1))
	//mock.ExpectBegin()
	//mock.ExpectCommit()
}

func Test_CreateQueueTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	client := &Client{&txExecutor{db, &sync.Mutex{}, nil}}

	mock.ExpectBegin()
	q := mock.ExpectQuery("[" + createQueueQuery + "]").WithArgs("js")
	q.WillReturnRows(sqlmock.NewRows([]string{"result"}).AddRow(1))
	mock.ExpectCommit()

	res, err := client.CreateQueue("js")
	assert.NotNil(t, res, "missing result")
	assert.Equal(t, 1, res, "unexpected result")
	assert.Nil(t, err, "unexpected error")

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err, "there were unfulfilled expections")

	mock.ExpectBegin()
	q = mock.ExpectQuery("[" + createQueueQuery + "]").WithArgs("js")
	q.WillReturnError(errors.New("tx error"))
	mock.ExpectRollback()

	res, err = client.CreateQueue("js")
	assert.NotNil(t, res, "missing result")
	assert.Equal(t, 0, res, "unexpected result")
	assert.NotNil(t, err, "unexpected error")

	err = mock.ExpectationsWereMet()
	assert.Nil(t, err, "there were unfulfilled expections")
	//	mock.ExpectExec("INSERT INTO product_viewers").WithArgs(2, 3).WillReturnResult(sqlmock.NewResult(1, 1))
}
