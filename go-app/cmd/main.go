package main

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/producer"
)

// TODO hardcode - вынести в конфиг?
const (
	topic       = "topic_unit_1"
	singleGroup = "single-group" // TODO hardcode - вынести в конфиг?
	batchGroup  = "batch-group"  // TODO hardcode - вынести в конфиг?
)

func main() {

	logThis := logger.New()

	var cfg config.Config
	cfg.Load(".env")
	// TODO del
	//logThis.Info("%#v", cfg.Producer)

	// создаем продюсера
	publisher, err := producer.NewProducer[models.Message](cfg, logThis, json.Marshal)
	if err != nil {
		logThis.Error("Ошибка создания продюсера: %v", err)
		return
	}
	defer publisher.Close()
	logThis.Info("Продюсер подключен к брокерам")

	countMsg := 1
	// Канал передачи сообщений между генератором и отправщиком
	produceChannel := make(chan *models.Message)

	// генерация сообщений:
	go generateMessage(produceChannel, countMsg)

	// отправка сообщений:
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for message := range produceChannel {
			errSending := publisher.SendMessage(topic, message)
			if errSending != nil {
				logThis.Error("Ошибка отправки сообщения (%v):\n%v", errSending, message)
			} else {
				logThis.Info("Сообщение отправлено:\n%v", message)
			}
		}
	}()

	wg.Wait()

	return

	/*
		1 создать продюсера
		Продюсер при создании:
			получает конфиг
			получает сериализатор

	*/
	/*
		ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// 1. Создаём продюсера (1 экземпляр)

			// 2. Запускаем продюсера в отдельной горутине
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				sendMessage(newProducer)
			}()

			// 3. Запускаем консьюмеров с уникальными group.id

			singleConsumer, err := consumer.NewSingleConsumer(brokers, singleGroup, topic)
			if err != nil {
				logging.Error("NewSingleConsumer ошибка", err)
				return
			}

			defer func() {
				if errClose := singleConsumer.Close(); errClose != nil {
					logging.Error("CRITICAL: Failed to close consumer gracefully: %v", errClose)
				}
			}()

			batchConsumer, err := consumer.NewBatchConsumer(brokers, batchGroup, topic)
			if err != nil {
				logging.Error("NewBatchConsumer ошибка", err)
				return
			}
			defer func() {
				if errClose := batchConsumer.Close(); errClose != nil {
					logging.Error("CRITICAL: Failed to close consumer gracefully: %v", errClose)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				singleConsumer.Consume(ctx)
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				batchConsumer.Consume(ctx)
			}()

			// 4. Ожидаем сигнала завершения
			sigChan := make(chan os.Signal, 1)
			signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
			<-sigChan

			logging.Info("Получен сигнал остановки...")
			cancel()
			wg.Wait()
			logging.Success("Приложение завершено")*/
}

// generateMessage - генерируем сообщения в количестве countMsg
func generateMessage(produceChannel chan<- *models.Message, countMsg int) {
	defer close(produceChannel)

	for i := 0; i < countMsg; i++ {
		msg := &models.Message{
			ID:      i,
			Payload: "Hello from producer",
			Ts:      time.Now().UnixNano(),
		}
		produceChannel <- msg
	}
}
