package producer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress"
	"time"
)

type KafkaProducer struct {
	writer *kafka.Writer
	config *Config
	Logger Logger
}

func NewkafkaProducer(config *Config, logger Logger) (*KafkaProducer, error) {
	writer := &kafka.Writer{
		Addr:     kafka.TCP(config.kafkaBrokers...),
		Topic:    config.Topic,
		Balancer: &kafka.Hash{},

		BatchSize:    config.BatchSize,
		BatchTimeout: config.FlushInterval,
		Compression:  compress.Snappy,

		RequiredAcks:    kafka.RequireAll,
		MaxAttempts:     config.RetryAttempts,
		WriteBackoffMax: config.RetryBackoff,

		Async:                  false,
		AllowAutoTopicCreation: false,

		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
			logger.Error("Kafka writer error", fmt.Errorf(msg, args), "producer", config.ProducerName)
		}),
	}

	kp := &KafkaProducer{
		writer: writer,
		config: config,
		Logger: logger,
	}
	logger.Info("kafka producer initialized", "producer", config.ProducerName, "topic", config.Topic, "brokers", config.kafkaBrokers)

	return kp, nil
}

func (kp *KafkaProducer) Publish(ctx context.Context, topic string, key []byte, values []byte) error {
	msg := &kafka.Message{
		Key:   key,
		Value: values,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "producer", Value: []byte(kp.config.ProducerName)},
			{Key: "topic", Value: []byte(topic)},
		},
	}

	if _, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	err := kp.writer.WriteMessages(ctx, *msg)
	if err != nil {
		kp.Logger.Error("Failed to publish message", err, "topic", topic, "key", string(key), "value", string(values), "error", err)
		return fmt.Errorf("Failed to publish message: %w", err)
	}

	kp.Logger.Debug("Message published", "topic", topic, "key", string(key), "value", string(values))

	return nil
}

func (kp *KafkaProducer) Ping() error {
	conn, err := kafka.Dial("tcp", kp.config.kafkaBrokers[0])
	if err != nil {
		return fmt.Errorf("Failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return fmt.Errorf("Failed to set deadline: %w", err)
	}

	partitions, err := conn.ReadPartitions(kp.config.Topic)

	if err != nil {
		return fmt.Errorf("Failed to read partitions: %w", err)
	}

	kp.Logger.Debug("Kafka health check passed", "topic", kp.config.Topic, "partitions", partitions)

	return nil
}

func (kp *KafkaProducer) Close() error {
	kp.Logger.Info("Closing kafka producer", "producer", kp.config.ProducerName)

	if err := kp.writer.Close(); err != nil {
		return fmt.Errorf("Failed to close writer: %w", err)
	}
	kp.Logger.Info("Kafka producer closed")
	return nil
}

func (kp *KafkaProducer) Stats() kafka.WriterStats {
	return kp.writer.Stats()
}
