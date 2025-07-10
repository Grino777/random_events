package storages

import (
	"context"
	"log/slog"

	"github.com/Grino777/random_events/internal/configs"
	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/storages/postgres"
	"github.com/Grino777/random_events/internal/storages/redis"
)

type DbStorage interface {
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
	Connect() error
	Close(ctx context.Context) error
}

type CacheStorage interface {
	RedisConnection
	RedisEvents
}

type RedisConnection interface {
	Connect(ctx context.Context) error
	Close(ctx context.Context) error
}

type RedisEvents interface {
	SaveFailedEvent(data []byte) error
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
	EventExists(senderId, payload string, createdAt int64) (bool, error)
}

type Storage struct {
	Db    DbStorage
	Redis CacheStorage
}

func NewStorages(log *slog.Logger, dbCfg configs.DbConfigs, rCfg configs.RedisConfigs) *Storage {
	return &Storage{
		Db:    postgres.NewPostgresStorage(log, dbCfg),
		Redis: redis.NewRedisStorage(log, rCfg),
	}
}
