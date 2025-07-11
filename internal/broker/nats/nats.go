package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	opNatsBroker = "broker.nats."
)

var (
	ErrJetStreamNotInitialized = errors.New("JetStream is not initialized")
	ErrJetStreamNotExist       = errors.New("JetStream is nil")
)

type DbClient interface {
	SaveProcessedEvent(event *models.Event) error
}

type Broker interface {
	Connect() error
	Close(ctx context.Context)
	PublishEvent(data []byte) error
	ListenStream(
		tasksChan chan models.BrokerMessage,
		messageCount int,
	)
}

type natsBroker struct {
	ctx       context.Context
	log       *slog.Logger
	conn      *nats.Conn
	js        jetstream.JetStream
	queueName string
	consumer  jetstream.Consumer
}

func NewNATSBroker(
	ctx context.Context,
	log *slog.Logger,
	queueName string,
) Broker {
	return &natsBroker{
		ctx:       ctx,
		log:       log,
		queueName: queueName,
	}
}

func (nb *natsBroker) Connect() error {
	conn, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		nb.log.Error("failed to connect to NATS", "error", err)
		return err
	}
	nb.conn = conn
	nb.log.Info("connected to NATS")

	if err := nb.initJetStream(); err != nil {
		return err
	}

	if err := nb.checkStream(); err != nil {
		return err
	}

	nb.log.Info("JetStream initialized")
	return nil
}

func (nb *natsBroker) Close(ctx context.Context) {
	if nb.conn != nil {
		nb.conn.Close()
	}
	nb.log.Error("connection with nats is nil")
}

func (nb *natsBroker) PublishEvent(data []byte) error {
	subject := nb.queueName
	if nb.js == nil {
		nb.log.Error("JetStream not initialized")
		return ErrJetStreamNotInitialized
	}

	_, err := nb.js.Publish(context.Background(), subject, data)
	if err != nil {
		nb.log.Error("failed to publish to JetStream", "subject", subject, "error", err)
		return err
	}
	nb.log.Debug("published to JetStream", "subject", subject)
	return nil
}

func (nb *natsBroker) checkStream() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	streamName := nb.queueName

	streamConfig := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{streamName, streamName + ".*"}, // Добавляем "events" явно
		Storage:   jetstream.FileStorage,
		Retention: jetstream.InterestPolicy, // Удаление после подтверждения
		MaxAge:    24 * time.Hour,           // Хранить сообщения 24 часа
		MaxBytes:  1024 * 1024 * 1024,       // Лимит 1 ГБ
	}

	_, err := nb.js.Stream(ctx, streamName)
	if err != nil {
		if errors.Is(jetstream.ErrStreamNotFound, err) {
			_, err := nb.js.CreateStream(ctx, streamConfig)
			if err != nil {
				nb.log.Error("failed to create stream", "stream", streamName, "error", err)
				return err
			}
			nb.log.Info("stream created", "stream", streamName)
		} else {
			nb.log.Error("failed to check stream", "stream", streamName, "error", err)
			return err
		}
	} else {
		nb.log.Info("stream already exists", "stream", streamName)
	}

	return nil
}

// Создает consumer для stream
func (nb *natsBroker) createConsumer() error {
	const op = opNatsBroker + "createConsumer"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	streamName := nb.queueName
	consumerName := "consumer_" + streamName
	consumer, err := nb.js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		FilterSubject: streamName,
	})
	if err != nil {
		nb.log.Error("failed to create consumer", "stream", streamName, "consumer", consumerName, logger.Error(err))
		return fmt.Errorf("%s: %v", op, err)
	}

	nb.log.Debug("consumer created", "queue", nb.queueName)
	nb.consumer = consumer
	return nil
}

func (nb *natsBroker) initJetStream() error {
	js, err := jetstream.New(nb.conn)
	if err != nil {
		nb.log.Error("failed to initialize JetStream", "error", err)
		return err
	}
	nb.js = js
	return nil
}
