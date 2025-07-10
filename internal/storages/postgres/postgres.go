package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Grino777/random_events/internal/configs"
	"github.com/Grino777/random_events/internal/domain/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	tableName = "success_events"
)

const (
	requestTimeout = 5 * time.Second
)

const pgOp = "storage.postgres.postgres."

type Storage interface {
	SaveProcessedEvent(ctx context.Context, event *models.Event) error
	Connect() error
	Close(ctx context.Context) error
}

type PostgresStorage struct {
	client *pgxpool.Pool
	log    *slog.Logger
	cfg    configs.DbConfigs
}

func NewPostgresStorage(
	log *slog.Logger,
	cfg configs.DbConfigs,
) Storage {
	return &PostgresStorage{
		log: log,
		cfg: cfg,
	}
}

func (ps *PostgresStorage) SaveProcessedEvent(ctx context.Context, event *models.Event) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("failed due to cancaled context")
	default:
		query := fmt.Sprintf(
			`INSERT INTO %s (sender_id, created_at, payload, exec_time) VALUES ($1, $2, $3, $4)`,
			pgx.Identifier{tableName}.Sanitize(),
		)
		_, err := ps.client.Exec(ctx, query, event.SenderId, event.CreatedAt, event.Payload, event.ExecTime)
		if err != nil {
			return fmt.Errorf("failed to save event to PostgreSQL: %v", err)
		}
		ps.log.Debug("event saved to database")
	}
	return nil
}
