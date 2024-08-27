package main

import (
	"context"
	"log"
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

	cqSender, err := session.NewSender(context.TODO(), "/queues/"+cq10.Name, nil)
	failOnError(err, "create AMQP 1.0 classic queue sender")

	qqSender, err := session.NewSender(context.TODO(), "/queues/"+qq10.Name, nil)
	failOnError(err, "create AMQP 1.0 quorum queue sender")

	payload := []byte("some payload")

	log.Println("starting publishers...")
	go func() {
		for true {
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
	}()

	go func() {
		for true {
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
	}()

	go func() {
		for true {
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
	}()

	go func() {
		for true {
			err := qqSender.Send(
				context.TODO(),
				&amqp10.Message{
					Header: &amqp10.MessageHeader{Durable: true},
					Data:   [][]byte{payload},
				},
				&amqp10.SendOptions{Settled: true},
			)
			failOnError(err, "AMQP 1.0 send to quorum queue")
		}
	}()

	time.Sleep(10 * time.Second)
	log.Println("stopping publishers...")
}
