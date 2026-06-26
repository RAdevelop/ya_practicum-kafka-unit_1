package producer

import (
	"fmt"

	"github.com/RAdevelop/ya_practicum-kafka-unit_1/go-app/internal/config"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Producer - продюсер для отправки сообщений в Kafka
type Producer[T any] struct {
	producer *kafka.Producer
	config   config.Config
}

// SendMessage - отправка сообщений в указаный топик
func (p *Producer[T]) SendMessage(topic string, msg *T) error {
	return nil
}

// Close - закрытие продюсера
func (p *Producer[T]) Close() {
	// Ждём доставки всех сообщений перед закрытием
	p.producer.Flush(p.config.Producer.FlushTimeoutMs)
	p.producer.Close()
}

// NewProducer - конструтор для продюсера
func NewProducer[T any](config config.Config) (*Producer[T], error) {

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.Producer.BootstrapServers,
		// Гарантия At Least Once
		"acks":               config.Producer.Acks,              // Подтверждение от всех реплик
		"retries":            config.Producer.Retries,           // Количество повторных попыток
		"retry.backoff.ms":   config.Producer.RetryBackoffMs,    // Пауза между попытками
		"enable.idempotence": config.Producer.EnableIdempotence, // Идемпотентность (защита от дублей)
		// Устанавливаем таймауты для подключения
		"socket.connection.setup.timeout.ms": config.Producer.SocketConnectionSetupTimeoutMs,
		"socket.timeout.ms":                  config.Producer.SocketTimeoutMs,
	})
	if err != nil {
		return nil, err
	}

	// Проверяем, что можем получить метаданные
	_, err = producer.GetMetadata(nil, false, config.Producer.SocketTimeoutMs)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("failed to connect to Kafka: %w", err)
	}

	return &Producer[T]{producer: producer, config: config}, nil
}

/*
// Send отправляет сообщение в топик (асинхронно)
func _Send(topic string, msg *models.Message) error {

	messageForProducer, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	// Асинхронная отправка
	err = pr.p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageForProducer,
	}, nil)

	if err != nil {
		return err
	}

	logger.Info("Отправлено: %s" + msg.String())
	return nil
}

// Close закрывает продюсер
func _Close() {
	pr.p.Flush(15 * 1000) // Ждём доставки всех сообщений
	pr.p.Close()
}
*/
