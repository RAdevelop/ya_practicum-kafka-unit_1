package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/consumer"
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

	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()

	logThis := logger.New("InMainApp")

	var cfg config.Config
	cfg.Load(".env")
	var wg sync.WaitGroup

	// создаем продюсера
	logProducer := logger.New("Producer")
	publisher, err := producer.NewProducer[models.Message](cfg, logProducer, json.Marshal)
	if err != nil {
		logProducer.Error("Error connecting the producer: %v", err)
		return
	}
	defer publisher.Close()
	logProducer.Info("Producer has been connected to the brokers")

	countMsg := 100
	// Канал передачи сообщений между генератором и отправщиком
	produceChannel := make(chan *models.Message, countMsg)

	// генерация сообщений:
	go generateMessage(produceChannel, countMsg)

	// отправка сообщений:

	wg.Add(3)
	go func() {
		defer wg.Done()
		for message := range produceChannel {
			errSending := publisher.SendMessage(topic, message)
			if errSending != nil {
				logProducer.Error("Error sending the message (%v):\n%v", errSending, message)
			} else {
				logProducer.Info("Message has been sent:\n%v", message)
			}
		}
	}()

	// создаем консьюмера для чтения сообщения по 10  шт
	loggerButchGroup := logger.New("ConsumerButchGroup")
	subscriberButchGroup, err := consumer.NewConsumer[models.Message](cfg, loggerButchGroup, json.Unmarshal, batchGroup, 10)
	if err != nil {
		loggerButchGroup.Error("Error on Consumer initialization: %v", err)
		return
	}

	defer func() {
		err = subscriberButchGroup.Close()
		if err != nil {
			loggerButchGroup.Error("Error on close: %v", err)
		}
	}()
	// подключаемся к топику
	err = subscriberButchGroup.SubscribeTopic(topic)
	if err != nil {
		loggerButchGroup.Error("Error on subscribe to a topic: %v", err)
	}
	loggerButchGroup.Info("Subscribed to a topic: %s", topic)

	// создаем консьюмера для чтения сообщения по одной шт
	loggerSingleGroup := logger.New("ConsumerSingleGroup")
	subscriberSingleGroup, err := consumer.NewConsumer[models.Message](cfg, loggerSingleGroup, json.Unmarshal, singleGroup, 1)

	if err != nil {
		loggerSingleGroup.Error("Error on initialization: %v", err)
		return
	}
	defer func() {
		err = subscriberSingleGroup.Close()
		if err != nil {
			loggerSingleGroup.Error("Error on close: %v", err)
		}
	}()
	// подключаемся к топику
	err = subscriberSingleGroup.SubscribeTopic(topic)
	if err != nil {
		loggerSingleGroup.Error("Error on subscribe to a topic: %v", err)
	}

	loggerSingleGroup.Info("Subscribed to a topic: %s", topic)

	/*
		processBatchCb - callback функция для обработки сообщений в процессе их получения из Кафки
	*/
	processBatchCbSingleGroup := func(ctx context.Context, messages []*models.Message) error {
		/*
			обработка сообщений, полученных из Кафка, например:
			- сохранние данных в БД
			- отправка в какой-нибудь сервисы
			- и тп
		*/
		//пока просто выведем сообщения:
		loggerSingleGroup.Info("Processing batch:\n%v", messages)

		return nil
	}
	/*
		processBatchCb - callback функция для обработки сообщений в процессе их получения из Кафки
	*/
	processBatchCbButchGroup := func(ctx context.Context, messages []*models.Message) error {
		/*
			обработка сообщений, полученных из Кафка, например:
			- сохранние данных в БД
			- отправка в какой-нибудь сервисы
			- и тп
		*/
		//пока просто выведем сообщения:
		loggerButchGroup.Info("Processing batch:\n%v", messages)

		return nil
	}

	go func() {
		defer wg.Done()
		subscriberSingleGroup.Consume(ctx, processBatchCbSingleGroup)
	}()

	go func() {
		defer wg.Done()
		subscriberButchGroup.Consume(ctx, processBatchCbButchGroup)
	}()

	//Обработка прерывания работы приложения, например, по CTR + c:
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logThis.Info("Interrupt signal received")
	ctxCancel()
	wg.Wait()
	logThis.Info("App is closed")

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
