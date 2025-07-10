package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Grino777/random_events/internal/broker"
	"github.com/Grino777/random_events/internal/configs"
	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
	"github.com/Grino777/random_events/internal/producer"
	"github.com/Grino777/random_events/internal/storages"
	"github.com/Grino777/random_events/internal/worker"
)

type Redis interface {
	RedisConnection
	RedisEvents
}

type RedisConnection interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

type RedisEvents interface {
	SaveFailedEvent(data []byte) error
	SaveProcessedEvent(context.Context, *models.Event) error
	EventExists(senderId, payload string, createdAt int64) (bool, error)
}

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

type Producer interface {
	Start() error
}

type Storage interface {
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
	Connect() error
	Close(ctx context.Context) error
}

type Workers interface {
	Start()
}

type App struct {
	ctx           context.Context
	log           *slog.Logger
	broker        Broker
	eventProducer Producer
	configs       *configs.Configs
	redis         Redis
	db            Storage
	workers       Workers
	Wg            sync.WaitGroup
}

func NewApp(ctx context.Context, log *slog.Logger) *App {
	return &App{
		log: log,
		ctx: ctx,
	}
}

func (a *App) Start(errChan chan error) error {
	configs, err := configs.NewConfigs()
	if err != nil {
		return err
	}
	a.configs = configs

	storage := storages.NewStorages(a.log, a.configs.DbConfig, a.configs.RedisConfig)

	a.db = storage.Db
	a.redis = storage.Redis

	if err := a.connectToRedis(); err != nil {
		return err
	}

	if err := a.connectToDb(); err != nil {
		return err
	}

	a.broker = broker.NewBroker(a.ctx, a.log, configs.NatsConfig.QueueName)
	if err := a.broker.Connect(); err != nil {
		return err
	}

	a.eventProducer = producer.NewProducer(a.ctx, a.log, a.broker)

	a.Wg.Add(1)
	go func() {
		defer a.Wg.Done()
		if err := a.eventProducer.Start(); err != nil {
			errChan <- err
		}
	}()

	a.workers = worker.NewWorkerStore(a.ctx, a.log, a.redis, a.db, a.broker)
	a.workers.Start()

	<-a.ctx.Done()
	a.Stop()
	return nil
}

func (a *App) Stop() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if a.redis != nil {
		if err := a.redis.Close(ctx); err != nil {
			a.log.Error("failed to close redis session", logger.Error(err))
			a.redis = nil
		}
	}
	if a.db != nil {
		if err := a.db.Close(ctx); err != nil {
			a.log.Error("failed to close db session", logger.Error(err))
			a.redis = nil
		}
	}
	if a.broker != nil {
		a.broker.Close(ctx)
	}
}

func (a *App) connectToRedis() error {
	if err := a.redis.Connect(a.ctx); err != nil {
		return err
	}
	return nil
}

func (a *App) connectToDb() error {
	if err := a.db.Connect(); err != nil {
		return err
	}
	return nil
}
