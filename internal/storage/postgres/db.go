// Package postgres хранит свечи в PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	// defaultMaxConns — верхняя граница пула. Агрегатор пишет из одного
	// потока, но соединение может подвиснуть на сети, и запас позволяет
	// следующему батчу не ждать. Больше двух на такой нагрузке бессмысленно.
	defaultMaxConns = 4

	defaultConnectTimeout = 10 * time.Second
)

// Config — параметры подключения.
type Config struct {
	DSN    string
	Logger *slog.Logger

	MaxConns       int32
	ConnectTimeout time.Duration
}

// DB — пул соединений с PostgreSQL.
type DB struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// New открывает пул и проверяет, что база отвечает.
//
// Проверка на старте обязательна: без неё опечатка в DSN всплыла бы только
// при записи первой свечи, через минуту после запуска, и выглядела бы как
// сбой конвейера, а не как ошибка конфигурации.
func New(ctx context.Context, cfg Config) (*DB, error) {
	if strings.TrimSpace(cfg.DSN) == "" {
		return nil, errors.New("postgres: empty DSN")
	}
	if cfg.Logger == nil {
		return nil, errors.New("postgres: logger is required")
	}

	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	if poolCfg.MaxConns <= 0 {
		poolCfg.MaxConns = defaultMaxConns
	}

	connectTimeout := cfg.ConnectTimeout
	if connectTimeout <= 0 {
		connectTimeout = defaultConnectTimeout
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	return &DB{pool: pool, log: cfg.Logger}, nil
}

// Close закрывает пул и ждёт завершения активных запросов.
func (db *DB) Close() {
	db.pool.Close()
}
