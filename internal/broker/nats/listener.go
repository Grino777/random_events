package nats

import (
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
	"github.com/nats-io/nats.go/jetstream"
)

func (nb *natsBroker) getConsumer() (jetstream.Consumer, error) {
	if nb.consumer != nil {
		return nb.consumer, nil
	}
	err := nb.createConsumer()
	if err != nil {
		return nil, err
	}
	return nb.consumer, nil
}

func (nb *natsBroker) ListenStream(
	tasksChan chan models.BrokerMessage, // Канал для отправки сообщений в WorkerStore
	messageCount int,
) {
	consumer, err := nb.getConsumer()
	if err != nil {
		nb.log.Error("failed to create consumer for broker", logger.Error(err))
		return
	}
	nb.log.Debug("consumer created", "queue", "events")

	for {
		msgs, err := consumer.Fetch(messageCount, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			nb.log.Error("failed to fetch broker messages", logger.Error(err))
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for msg := range msgs.Messages() {
			// Проверяем, закрыт ли tasksChan, перед отправкой
			select {
			case _, ok := <-tasksChan:
				if !ok {
					nb.log.Debug("stream listener stopped due to closed tasksChan")
					if err := msg.Nak(); err != nil {
						nb.log.Error("failed to nack message", logger.Error(err))
					}
					return
				}
				tasksChan <- models.NewBrokerMessage(msg)
			default:
				// Пытаемся отправить сообщение, но канал может быть полон
				select {
				case tasksChan <- models.NewBrokerMessage(msg):
					// Успешная отправка
				default:
					// tasksChan полон, отклоняем сообщение
					nb.log.Debug("tasksChan full, rejecting message")
					if err := msg.Nak(); err != nil {
						nb.log.Error("failed to nack message", logger.Error(err))
					}
				}
			}
		}
	}
}
