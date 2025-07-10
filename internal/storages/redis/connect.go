package redis

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Grino777/random_events/internal/lib/logger"
	"github.com/redis/go-redis/v9"
)

func (rs *RedisStorage) Connect(ctx context.Context) error {
	const op = opRedis + "Connect"

	log := rs.logger.With(slog.String("op", op))

	if err := rs.connectWithRetry(rs.cfg.MaxRetries); err != nil {
		log.Error("failed to connect to Redis after retries", logger.Error(err))
		return fmt.Errorf("%s: %w", op, err)
	}

	log.Debug("redis connection established")

	return nil
}

func (rs *RedisStorage) connectWithRetry(maxAttempts int) error {
	const op = opRedis + "connectWithRetry"

	log := rs.logger.With(slog.String("op", op))

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rs.mu.Lock()
		if rs.client != nil {
			if err := rs.client.Close(); err != nil {
				log.Warn("failed to close old Redis client", logger.Error(err))
			}
			rs.client = nil
		}

		options := &redis.Options{
			Addr:         rs.cfg.Addr,
			Password:     rs.cfg.Password,
			Username:     rs.cfg.User,
			DB:           rs.cfg.DB,
			MaxRetries:   rs.cfg.MaxRetries,
			DialTimeout:  rs.cfg.DialTimeout,
			ReadTimeout:  rs.cfg.Timeout,
			WriteTimeout: rs.cfg.Timeout,
		}

		client := redis.NewClient(options)
		rs.mu.Unlock()

		// Проверка соединения
		ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.DialTimeout)
		if err := client.Ping(ctx).Err(); err != nil {
			cancel() // Закрываем контекст сразу после ошибки
			rs.mu.Lock()
			if closeErr := client.Close(); closeErr != nil {
				log.Warn("failed to close Redis client after failed ping", logger.Error(closeErr))
			}
			rs.mu.Unlock()
			lastErr = err
			log.Warn("failed to connect to Redis",
				slog.Int("attempt", attempt),
				slog.Any("error", lastErr),
				slog.Duration("retryAfter", rs.cfg.RetryDelay),
			)
			if attempt < maxAttempts {
				time.Sleep(rs.cfg.RetryDelay)
			}
			continue
		}
		cancel() // Закрываем контекст после успешного пинга

		rs.mu.Lock()
		rs.client = client
		rs.mu.Unlock()

		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", maxAttempts, lastErr)
}

func (rs *RedisStorage) Close(ctx context.Context) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.client != nil {
		if err := rs.client.Close(); err != nil {
			return fmt.Errorf("failed to close redis client: %w", err)
		}
		rs.client = nil
	}
	rs.logger.Debug("redis connection is closed")
	return nil
}

func withClient[T any](rs *RedisStorage, fn func(*redis.Client) (T, error)) (T, error) {
	var zero T
	rs.mu.RLock()
	client := rs.client
	rs.mu.RUnlock()

	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.Timeout)
	defer cancel()

	if client == nil || client.Ping(ctx).Err() != nil {
		rs.logger.Warn("redis connection lost, attempting reconnect")
		if err := rs.connectWithRetry(rs.cfg.MaxRetries); err != nil {
			return zero, fmt.Errorf("failed to reconnect: %w", err)
		}
		rs.mu.RLock()
		client = rs.client
		rs.mu.RUnlock()
	}

	result, err := fn(client)
	if err != nil {
		return zero, err
	}
	return result, nil
}
