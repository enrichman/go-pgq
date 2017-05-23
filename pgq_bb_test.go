package pgq_test

import (
	"testing"

	"fmt"

	pgq "github.com/enrichman/go-pgq"
)

func Test_Connection(t *testing.T) {
	connString := "postgres://postgres:password@localhost:5432?sslmode=disable"
	client, err := pgq.NewClient(connString, true)

	fmt.Println(client, err)

	res, err := client.CreateQueue("js")
	fmt.Printf("bla %+v %+v\n", res, err)
	queueInfo, err := client.GetQueueInfo("js")
	fmt.Printf("queueInfo %+v %+v\n", queueInfo, err)

	consumer, err := pgq.NewConsumer(connString, true, "notifications", "consumer")
	batchID, err := consumer.NextBatch()
	fmt.Println("batch", batchID, err)
}
