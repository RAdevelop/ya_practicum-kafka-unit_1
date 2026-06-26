package main

import (
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/config"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/logger"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/models"
	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/producer"
)

// TODO hardcode - вынести в конфиг?
const (
	brokers     = "kafka-b-1:9092,kafka-b-2:9092,kafka-b-3:9092" // брокеры
	topic       = "topic_unit_1"
	singleGroup = "single-group" // TODO hardcode - вынести в конфиг?
	batchGroup  = "batch-group"  // TODO hardcode - вынести в конфиг?
)

func main() {

	var cfg config.Config
	cfg.Load(".env")
	fmt.Printf("%#v", cfg.Producer)
	fmt.Println("")

	publisher, err := producer.NewProducer[models.Message](cfg)
	if err != nil {
		logger.Error("Ошибка создания продюсера: ", err)
		return
	}
	defer publisher.Close()

	logger.Info("Продюсер подключен к брокерам")

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
				logger.Error("NewSingleConsumer ошибка", err)
				return
			}

			defer func() {
				if errClose := singleConsumer.Close(); errClose != nil {
					logger.Error("CRITICAL: Failed to close consumer gracefully: %v", errClose)
				}
			}()

			batchConsumer, err := consumer.NewBatchConsumer(brokers, batchGroup, topic)
			if err != nil {
				logger.Error("NewBatchConsumer ошибка", err)
				return
			}
			defer func() {
				if errClose := batchConsumer.Close(); errClose != nil {
					logger.Error("CRITICAL: Failed to close consumer gracefully: %v", errClose)
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

			logger.Info("Получен сигнал остановки...")
			cancel()
			wg.Wait()
			logger.Success("Приложение завершено")*/
}

/*
// TODO сделать метод, который просто вернет (генерация) slice сообщений
func sendMessage(p *producer.Producer) {
	for i := 0; i < 100; i++ {
		msg := &models.Message{
			ID:      i,
			Payload: "Hello from producer",
			Ts:      time.Now().Unix(),
		}
		if err := p.Send(topic, msg); err != nil {
			logger.Error("Ошибка отправки: %v", err)
		}
		// TODO для отладки и наблюдения за сообщениями в консоли
		time.Sleep(1 * time.Second)
	}
}
*/
