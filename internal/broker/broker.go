package broker

import (
	"context"
	"log/slog"

	cNats "github.com/Grino777/random_events/internal/broker/nats"
	"github.com/Grino777/random_events/internal/domain/models"
)

type Broker interface {
	Connect() error
	Close(ctx context.Context)
	PublishEvent(data []byte) error
	ListenStream(
		ctx context.Context,
		tasksChan chan models.EventMessage,
		messageCount int,
	)
}

type RedisClient interface {
	SaveProcessedEvent(event *models.Event) error
	EventExists(senderId, payload string, createdAt int64) (bool, error)
}

type DbClient interface {
	SaveProcessedEvent(event *models.Event) error
}

type brokerObj struct {
	log    *slog.Logger
	broker Broker
}

func NewBroker(
	ctx context.Context,
	log *slog.Logger,
	queueName string,
) Broker {
	return &brokerObj{
		log:    log,
		broker: cNats.NewNATSBroker(ctx, log, queueName),
	}
}

func (b *brokerObj) Connect() error {
	if err := b.broker.Connect(); err != nil {
		return err
	}
	return nil
}

func (b *brokerObj) Close(ctx context.Context) {
	if b.broker != nil {
		b.broker.Close(ctx)
	}
	b.log.Debug("connection with message broker closed")
}

func (b *brokerObj) PublishEvent(data []byte) error {
	if err := b.broker.PublishEvent(data); err != nil {
		return err
	}
	return nil
}

func (b *brokerObj) ListenStream(
	ctx context.Context,
	tasksChan chan models.EventMessage,
	messageCount int,
) {
	b.broker.ListenStream(ctx, tasksChan, messageCount)
}
