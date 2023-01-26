package main

import (
	"fmt"
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main () {
    topic := "myTopic"
    // Producer
    p, err := kafka.NewProducer(&kafka.ConfigMap{
        "bootstrap.servers": "localhost:9092",
        "client.id": "something",
        "acks": "all"})

    if err != nil {
        fmt.Printf("Failed to create producer: %s\n", err)
    }

    delivery_chan := make(chan kafka.Event, 10000)
    err = p.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
        Value: []byte("payload")},
        delivery_chan,
    )

    if err != nil {
        log.Fatal(err)
    }

    e := <-delivery_chan
    fmt.Println(e)

    // consumer

    go func () {

        c, err := kafka.NewConsumer(&kafka.ConfigMap{
            "bootstrap.servers": "localhost:9092",
            "group.id": "myGroup",
            "auto.offset.reset": "smallest"})

        if err != nil {
            log.Fatal(err)
        }

        err = c.Subscribe(topic, nil)

        if err != nil {
            log.Fatal(err)
        }

        for {
            ev := c.Poll(100)
            switch e := ev.(type) {
            case *kafka.Message:
                    // application-specific processing
            case kafka.Error:
                    fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
            default:
                    fmt.Printf("Ignored %v\n", e)
            }
        }

    }() 
}


