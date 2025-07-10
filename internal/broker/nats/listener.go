package nats

import (
	"context"
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
	"github.com/nats-io/nats.go/jetstream"
)

func (nb *natsBroker) ListenStream(
	ctx context.Context,
	tasksChan chan models.EventMessage,
	messageCount int,
) {
	consumer, err := nb.getConsumer()
	if err != nil {
		nb.log.Error("failed to create consumer for broker", logger.Error(err))
		return
	}
	nb.log.Debug("consumer created", "queue", "events")

	for {
		select {
		case <-ctx.Done():
			nb.log.Debug("stream listener stopped")
			return
		default:

			msgs, err := consumer.Fetch(messageCount, jetstream.FetchMaxWait(5*time.Second))
			if err != nil {
				nb.log.Error("failed to fetch broker messages", logger.Error(err))
				time.Sleep(500 * time.Millisecond)
				continue
			}
			for msg := range msgs.Messages() {
				select {
				case <-ctx.Done():
					return
				case tasksChan <- models.NewBrokerMessage(msg):
					// Успешная отправка сообщения в tasksChan
				}

			}
		}
	}
}
