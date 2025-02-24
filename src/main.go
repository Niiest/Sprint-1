// main.go
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	// Проверяем, что количество параметров при запуске программы ровно 4
	if len(os.Args) != 4 {
		log.Fatalf("Пример использования: %s <bootstrap-servers> <topic> <producing-delay>\n", os.Args[0])
	}

	// Парсим параметры и получаем адрес брокера и имя топика
	bootstrapServers := os.Args[1]
	topic := os.Args[2]
	producingDelayMs, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatalf("Ошибка в параметре задержки генерации событий : %s: %v\n", os.Args[3], err)
	}

	producingDelay := time.Duration(producingDelayMs) * time.Millisecond

	producer := RunProducer(bootstrapServers, topic, producingDelay)

	// Перехватываем сигналы syscall.SIGINT и syscall.SIGTERM для graceful shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	polling_consumer := RunPullingConsumer(bootstrapServers, []string{topic})

	event_consumer := RunEventConsumer(bootstrapServers, []string{topic})

	sig := <-sigchan
	fmt.Printf("Передан сигнал %v: приложение останавливается\n", sig)

	producer.Close()
	polling_consumer.Close()
	event_consumer.Close()
}
