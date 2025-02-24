package main

import (
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

func RunProducer(bootstrapServers string, topic string, delay time.Duration) *kafka.Producer {
	// Создаём продюсера
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"acks":              "all",
		"retries":           3,
	})

	if err != nil {
		log.Fatalf("Невозможно создать продюсера: %s\n", err)
	}

	log.Printf("Продюсер создан %v\n", p)

	go ProduceOrdersInLoop(p, topic, delay)

	return p
}

func ProduceOrdersInLoop(p *kafka.Producer, topic string, delay time.Duration) {
	// Канал доставки событий (информации об отправленном сообщении)
	deliveryChan := make(chan kafka.Event)

	for {
		// Создаём заказ
		value := &Order{
			OrderID: uuid.New().String(),
			UserID:  uuid.New().String(),
			Items: []Item{
				{ProductID: "535", Quantity: 1, Price: 300},
				{ProductID: "125", Quantity: 2, Price: 100},
			},
			TotalPrice: 500.00,
		}

		payload, err := Serialize(value)
		if err != nil {
			log.Fatalf("Невозможно сериализовать заказ: %s\n", err)
		}

		log.Printf("Отправляется сообщение:\n%+v\n", value)

		// Отправляем сообщение в брокер
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          payload,
			Headers:        []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
		}, deliveryChan)

		if err != nil {
			log.Fatalf("Ошибка при отправке сообщения: %v\n", err)
		}

		e := <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			log.Printf("Отправка не удалась: %v\n", m.TopicPartition.Error)
		} else {
			log.Printf("Отправлено сообщение в топик %s [%d]\n", *m.TopicPartition.Topic, m.TopicPartition.Partition)
		}

		log.Printf("Отправлен заказ:\n%+v\n", value)

		time.Sleep(delay)
	}
}
