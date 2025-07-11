package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	opPostgres = "storages.postgres."
)

func (ps *PostgresStorage) Connect() error {
	const op = opPostgres + "Connect"

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	client, err := ps.getDbPool(ctx)
	if err != nil {
		return err
	}
	ps.client = client

	if err := ps.createDatabase(ctx); err != nil {
		return err
	}

	// Создание базы данных и таблицы
	if err := ps.createTable(); err != nil {
		if closeErr := ps.Close(ctx); closeErr != nil {
			return fmt.Errorf("%s: failed to create database/table: %w; close error: %v", op, err, closeErr)
		}
		return fmt.Errorf("%s: failed to create database/table: %w", op, err)
	}

	ps.log.Debug("postgres connection established")
	return nil
}

func (ps *PostgresStorage) Close(ctx context.Context) error {
	const op = opPostgres + "Close"

	if ps.client == nil {
		return nil
	}

	ps.client.Close()
	ps.log.Debug("postgres connection pool closed")
	return nil
}

func (ps *PostgresStorage) getDbPool(ctx context.Context) (*pgxpool.Pool, error) {
	const op = opPostgres + "getDbPool"

	// Подключение к системной базе для проверки/создания базы events
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/postgres?sslmode=disable",
		ps.cfg.User, ps.cfg.Password, ps.cfg.Host, ps.cfg.Port)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to connect to Postgres: %w", op, err)
	}

	// Настройка пула соединений
	config.MaxConns = 20
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute

	client, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to connect to Postgres: %w", op, err)
	}

	return client, nil
}

func (ps *PostgresStorage) createDatabase(ctx context.Context) error {
	const op = opPostgres + "createDatabase"

	var exists bool
	err := ps.client.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", ps.cfg.DbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("%s: failed to check database existence: %w", op, err)
	}

	if exists {
		ps.log.Debug("database already exists", slog.String("db_name", ps.cfg.DbName))
		return nil
	}

	dbName := pgx.Identifier{ps.cfg.DbName}.Sanitize()
	stmt := fmt.Sprintf("CREATE DATABASE %s", dbName)
	_, err = ps.client.Exec(ctx, stmt)
	if err != nil {
		return fmt.Errorf("%s: failed to create database %s: %w", op, ps.cfg.DbName, err)
	}

	ps.log.Info("database created", slog.String("db_name", ps.cfg.DbName))
	return nil
}

func (ps *PostgresStorage) createTable() error {
	const op = opPostgres + "createDb"

	// Создаем контекст с таймаутом
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	createTableQuery := fmt.Sprintf(
		`
		CREATE TABLE IF NOT EXISTS %s (
			id SERIAL PRIMARY KEY,
			sender_id VARCHAR(255) NOT NULL,
			created_at BIGINT NOT NULL,
			payload TEXT NOT NULL,
			exec_time FLOAT NOT NULL,
			UNIQUE(sender_id, created_at, payload)
		);`,
		pgx.Identifier{tableName}.Sanitize(),
	)
	_, err := ps.client.Exec(ctx, createTableQuery)
	if err != nil {
		ps.client.Close()
		return fmt.Errorf("%s: failed to create successEvent table: %w", op, err)
	}
	ps.log.Debug("table ensured")

	return nil
}
