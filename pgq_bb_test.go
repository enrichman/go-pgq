package pgq_test

import (
	"testing"

	"fmt"

	pgq "github.com/enrichman/go-pgq"
)

func Test_Connection(t *testing.T) {
	connString := "postgres://postgres:password@localhost:5432?sslmode=disable"
	client, err := pgq.NewClient(connString, pgq.WithTxEnabled(true))

	fmt.Println(client, err)

	res, err := client.CreateQueue("js")
	fmt.Printf("bla %+v %+v\n", res, err)
	queueInfo, err := client.GetQueueInfo("js")
	fmt.Printf("queueInfo %+v %+v\n", queueInfo, err)

	batchID, err := client.NextBatch("notifications", "consumer")
	fmt.Println("batch", batchID, err)
}
