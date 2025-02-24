package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func CreateConsumer(bootstrapServers string, group string, topics []string, autocommmit bool) *kafka.Consumer {
	// Создаём консьюмера
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  bootstrapServers,
		"group.id":           group, // уникальный идентификатор группы
		"session.timeout.ms": 6000,  // время, в течение которого Kafka ожидает активности от консьюмера до того, как объявит его «мёртвым» и начнёт ребалансировку
		"enable.auto.commit": autocommmit,
		"fetch.min.bytes":    1024,
		"auto.offset.reset":  "earliest"}) // начинать чтение с самого старого доступного сообщения.

	if err != nil {
		log.Fatalf("Невозможно создать консьюмера: %s\n", err)
	}

	// Подписываемся на топики, в примере он один
	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		log.Fatalf("Невозможно подписаться на топик: %s\n", err)
	}

	return c
}

func RunPullingConsumer(bootstrapServers string, topics []string) *kafka.Consumer {
	// Pull-consumer будет самостоятельно коммитить оффсет
	autocommit := false

	// Выделяем в отдельную группу, чтобы чтение сообщений шло независимо от второго консьюмера
	group := "pull"
	c := CreateConsumer(bootstrapServers, group, topics, autocommit)

	fmt.Printf("Pull-консьюмер создан %v\n", c)

	go RunConsumer(c, group, 500, !autocommit)

	return c
}

func RunEventConsumer(bootstrapServers string, topics []string) *kafka.Consumer {
	// Event-consumer будет автоматически коммитить оффсет
	autocommit := true

	// Выделяем в отдельную группу, чтобы чтение сообщений шло независимо от второго консьюмера
	group := "event"
	c := CreateConsumer(bootstrapServers, group, topics, autocommit)

	fmt.Printf("Event-консьюмер создан %v\n", c)

	// Выставляем как можно больший таймаут на чтение сообщений для эмуляции push-модели
	go RunConsumer(c, group, 10000000, !autocommit)

	return c
}

func RunConsumer(c *kafka.Consumer, group string, timeoutMs int, manualCommit bool) {
	for {
		// Делаем запрос на считывание сообщения из брокера
		ev := c.Poll(timeoutMs)
		if ev == nil {
			log.Printf("Сообщений нет для %s-консьюмера, повтор", group)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		log.Printf("Получено сообщение %s-консьюмером", group)

		//     Приводим Events к
		switch e := ev.(type) {
		// типу *kafka.Message,
		case *kafka.Message:
			value := Order{}
			err := Deserialize(e.Value, &value)
			if err != nil {
				log.Printf("Ошибка десериализации: %s\n", err)
			} else {
				log.Printf("Консьюмером получено сообщение в топик %s:\n%+v\n", e.TopicPartition, value)
				if manualCommit {
					c.Commit()
					log.Println("Оффсет закоммичен вручную")
				}
			}

		// типу Ошибки брокера
		case kafka.Error:
			// Ошибки обычно следует считать
			// информационными, клиент попытается
			// автоматически их восстановить
			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
		default:
			fmt.Printf("Другие события %v\n", e)
		}
	}
}
