package main

import (
	"context"
	"log"
	"sync"
	"time"

	amqp10 "github.com/Azure/go-amqp"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func main() {
	numMessages := 1_000_000
	numUnconfirmedPublishes := 10_000
	numPrefetch := 200

	conn10, err := amqp10.Dial(
		context.TODO(),
		"amqp://localhost",
		&amqp10.ConnOptions{SASLType: amqp10.SASLTypeAnonymous()})
	failOnError(err, "open AMQP 1.0 connection")

	conn091, err := amqp091.Dial("amqp://guest:guest@localhost")
	failOnError(err, "open AMQP 0.9.1 connection")

	session, err := conn10.NewSession(context.TODO(), nil)
	failOnError(err, "begin AMQP 1.0 session")

	ch, err := conn091.Channel()
	failOnError(err, "open AMQP 0.9.1 channel")

	cq091, err := ch.QueueDeclare(
		"classic-queue-amqp-091", // name
		true,                     // durable
		false,                    // delete when unused
		false,                    // exclusive
		false,                    // no-wait
		amqp091.Table{"x-queue-type": "classic"},
	)
	failOnError(err, "declare classic queue")

	cq10, err := ch.QueueDeclare(
		"classic-queue-amqp-10", // name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		amqp091.Table{"x-queue-type": "classic"},
	)
	failOnError(err, "declare classic queue")

	qq091, err := ch.QueueDeclare(
		"quorum-queue-amqp-091", // name
		true,                    // durable
		false,                   // delete when unused
		false,                   // exclusive
		false,                   // no-wait
		amqp091.Table{"x-queue-type": "quorum"},
	)
	failOnError(err, "declare quorum queue")

	qq10, err := ch.QueueDeclare(
		"quorum-queue-amqp-10", // name
		true,                   // durable
		false,                  // delete when unused
		false,                  // exclusive
		false,                  // no-wait
		amqp091.Table{"x-queue-type": "quorum"},
	)
	failOnError(err, "declare quorum queue")

	payload := []byte("some payload")

	for i := 0; i < numMessages; i++ {
		err := ch.Publish(
			"",         // exchange
			cq091.Name, // routing key
			true,       // mandatory
			false,      // immediate
			amqp091.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp091.Persistent,
				Body:         payload,
			},
		)
		failOnError(err, "AMQP 0.9.1 publish to classic queue")
	}
	log.Println("published 1 million messages via AMQP 0.9.1 to classic queue")

	cqSender, err := session.NewSender(context.TODO(), "/queues/"+cq10.Name, nil)
	failOnError(err, "create AMQP 1.0 classic queue sender")

	for i := 0; i < numMessages; i++ {
		err := cqSender.Send(
			context.TODO(),
			&amqp10.Message{
				Header: &amqp10.MessageHeader{Durable: true},
				Data:   [][]byte{payload},
			},
			&amqp10.SendOptions{Settled: true},
		)
		failOnError(err, "AMQP 1.0 send to classic queue")
	}
	log.Println("published 1 million messages via AMQP 1.0 to classic queue")

	err = cqSender.Close(context.TODO())
	failOnError(err, "close AMQP 1.0 classic queue sender")

	qqSender, err := session.NewSender(context.TODO(), "/queues/"+qq10.Name, nil)
	failOnError(err, "create AMQP 1.0 quorum queue sender")

	err = ch.Confirm(false)
	failOnError(err, "put channel in confirm mode")
	log.Println("put channel in confirm mode")

	confirms := ch.NotifyPublish(make(chan amqp091.Confirmation, numUnconfirmedPublishes))

	err = ch.Qos(numPrefetch, 0, false)
	failOnError(err, "setting QoS")
	log.Printf("set QoS=%d on channel", numPrefetch)

	msgs, err := ch.Consume(
		cq091.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "consume from classic queue")
	log.Println("registered AMQP 0.9.1 classic queue consumer")

	cqReceiver, err := session.NewReceiver(
		context.TODO(),
		"/queues/"+cq10.Name,
		&amqp10.ReceiverOptions{Credit: int32(numPrefetch)},
	)
	failOnError(err, "create AMQP 1.0 classic queue receiver")
	log.Println("registered AMQP 1.0 classic queue consumer")

	log.Printf(
		"starting quorum queue publishers; sending in batches of %d unconfirmed publishes\n",
		numUnconfirmedPublishes)
	go func() {
		for true {
			for i := 0; i < numUnconfirmedPublishes; i++ {
				err = ch.Publish(
					"",         // exchange
					qq091.Name, // routing key
					true,       // mandatory
					false,      // immediate
					amqp091.Publishing{
						ContentType:  "text/plain",
						DeliveryMode: amqp091.Persistent,
						Body:         payload,
					},
				)
				failOnError(err, "AMQP 0.9.1 publish to quorum queue")
			}
			for i := 0; i < numUnconfirmedPublishes; i++ {
				if confirmed := <-confirms; !confirmed.Ack {
					log.Panicf("broker confirm error: %+v", confirmed)
				}
			}
		}
	}()
	go func() {
		for true {
			var wg sync.WaitGroup
			for i := 0; i < numUnconfirmedPublishes; i++ {
				wg.Add(1)
				// Since every Send blocks until broker confirms the message,
				// we have to open a separate goroutine per message:
				// https://github.com/Azure/go-amqp/issues/331
				go func() {
					defer wg.Done()
					// Send is safe for concurrent use.
					err := qqSender.Send(
						context.TODO(),
						&amqp10.Message{
							Header: &amqp10.MessageHeader{Durable: true},
							Data:   [][]byte{payload},
						},
						&amqp10.SendOptions{Settled: false},
					)
					failOnError(err, "AMQP 1.0 send to quorum queue")
				}()
			}
			wg.Wait()
		}
	}()

	log.Println("starting classic queue consumers...")
	go func() {
		for msg := range msgs {
			err = ch.Ack(msg.DeliveryTag, false)
			failOnError(err, "ack")
		}
	}()
	go func() {
		for true {
			msg, err := cqReceiver.Receive(context.TODO(), nil)
			failOnError(err, "AMQP 1.0 receive from classic queue")
			err = cqReceiver.AcceptMessage(context.TODO(), msg)
			failOnError(err, "AMQP 1.0 accept message")
		}
	}()

	time.Sleep(10 * time.Second)
	log.Println("stopping benchmark...")
}
