package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"
	"time"

	"github.com/Grino777/random_events/internal/configs"
	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/Grino777/random_events/internal/lib/logger"
	"github.com/redis/go-redis/v9"
)

var (
	ErrCacheNotFound = errors.New("data not cached")
)

const (
	failedEvents  = "failedEvents"
	successEvents = "successEvent"
	uniqueEvents  = "uniqueEvents"
)

const (
	failedEventsTTL = 24 * time.Hour
	successEventTTL = 1 * time.Hour
)

const (
	opRedis = "storage.redis."
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
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
	DeleteProcessedEvent(ctx context.Context, event *models.Event) error
	EventExists(senderId, payload string, createdAt int64) (bool, error)
}

type RedisStorage struct {
	mu     sync.RWMutex
	cfg    configs.RedisConfigs
	client *redis.Client
	logger *slog.Logger
}

func NewRedisStorage(log *slog.Logger, cfg configs.RedisConfigs) Redis {
	store := &RedisStorage{
		cfg:    cfg,
		logger: log,
	}
	return store
}

func (rs *RedisStorage) SaveFailedEvent(data []byte) error {
	_, err := withClient(rs, func(rc *redis.Client) (models.BrokerMessage, error) {
		ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.Timeout)
		defer cancel()

		key := fmt.Sprintf("%s:%d", failedEvents, time.Now().UnixNano())
		_, err := rc.Set(ctx, key, data, failedEventsTTL).Result()
		if err != nil {
			return nil, err
		}
		return nil, nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (rs *RedisStorage) SaveProcessedEvent(ctx context.Context, event *models.Event) error {
	const op = opRedis + "SaveProcessedEvent"

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		_, err := withClient(rs, func(rc *redis.Client) (models.BrokerMessage, error) {
			ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.Timeout)
			defer cancel()

			eventJSON, err := json.Marshal(event)
			if err != nil {
				return nil, fmt.Errorf("%s: %v", op, err)
			}

			// Генерация уникального хэша для event
			eventKey := fmt.Sprintf("%d:%s", event.CreatedAt, hashPayload(event.Payload))

			// Ключи для Redis
			hashKey := fmt.Sprintf("events:%s", event.SenderId)
			setKey := fmt.Sprintf("unique:%s", event.SenderId)

			// Сохранение в Hash (все события SenderId)
			err = rc.HSet(ctx, hashKey, eventKey, eventJSON).Err()
			if err != nil {
				return nil, fmt.Errorf("failed to save event to hash: %v", err)
			}

			// Сохранение в Set для проверки уникальности
			err = rc.SAdd(ctx, setKey, eventKey).Err()
			if err != nil {
				return nil, fmt.Errorf("failed to save event to set: %v", err)
			}

			// Установка TTL (например, 7 дней) для экономии памяти
			err = rc.Expire(ctx, hashKey, 7*24*time.Hour).Err()
			if err != nil {
				return nil, fmt.Errorf("failed to set TTL for hash: %v", err)
			}
			err = rc.Expire(ctx, setKey, 7*24*time.Hour).Err()
			if err != nil {
				return nil, fmt.Errorf("failed to set TTL for set: %v", err)
			}

			return nil, nil
		})
		if err != nil {
			return err
		}
		return nil
	}

}

func (rs *RedisStorage) DeleteProcessedEvent(ctx context.Context, event *models.Event) error {
	const op = opRedis + "DeleteProcessedEvent"

	_, err := withClient(rs, func(rc *redis.Client) (any, error) {
		ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.Timeout)
		defer cancel()

		// Генерация ключа события
		eventKey := fmt.Sprintf("%d:%s", event.CreatedAt, hashPayload(event.Payload))

		// Ключи для Redis
		hashKey := fmt.Sprintf("events:%s", event.SenderId)
		setKey := fmt.Sprintf("unique:%s", event.SenderId)

		// Удаление из Hash
		err := rc.HDel(ctx, hashKey, eventKey).Err()
		if err != nil {
			return nil, fmt.Errorf("%s: failed to delete event from hash: %v", op, err)
		}

		// Удаление из Set
		err = rc.SRem(ctx, setKey, eventKey).Err()
		if err != nil {
			return nil, fmt.Errorf("%s: failed to delete event from set: %v", op, err)
		}

		return nil, nil
	})
	if err != nil {
		rs.logger.Error("failed to delete processed event", logger.Error(err))
		return err
	}
	return nil
}

// EventExists проверяет, существует ли событие с данным Payload для SenderId
func (rs *RedisStorage) EventExists(senderId, payload string, createdAt int64) (bool, error) {
	setKey := fmt.Sprintf("%s:%s", uniqueEvents, senderId)
	eventKey := fmt.Sprintf("%d:%s", createdAt, hashPayload(payload))

	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.Timeout)
	defer cancel()

	exists, err := rs.client.SIsMember(ctx, setKey, eventKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check event existence: %v", err)
	}

	return exists, nil
}

// hashPayload создает хэш от Payload для каждого event
func hashPayload(payload string) string {
	h := fnv.New64a()
	h.Write([]byte(payload))
	return fmt.Sprintf("%x", h.Sum64())
}
