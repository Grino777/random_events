package producer

import (
	"context"
	"log/slog"
	"time"

	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
)

const (
	eventTTL = 500 * time.Millisecond
)

type Broker interface {
	PublishEvent(data []byte) error
}

type Producer interface {
	Start() error
}

type producerObj struct {
	log    *slog.Logger
	ctx    context.Context
	broker Broker
}

func NewProducer(ctx context.Context, log *slog.Logger, broker Broker) Producer {
	return &producerObj{
		log:    log,
		ctx:    ctx,
		broker: broker,
	}
}

func (p *producerObj) Start() error {
	p.run()
	return nil
}

func (p *producerObj) run() error {
	for {
		select {
		case <-p.ctx.Done():
			p.log.Debug("producer stopped by context")
			return nil
		default:
			time.Sleep(eventTTL)
			event, err := models.NewEvent()
			if err != nil {
				p.log.Debug("failed to genereate new event")
				continue
			}

			data, err := event.ToJSON()
			if err != nil {
				continue
			}

			if err := p.broker.PublishEvent(data); err != nil {
				p.log.Error("failed to send message to queue", logger.Error(err))
			} else {
				p.log.Debug("message successfully added to queue")
			}
		}
	}
}
