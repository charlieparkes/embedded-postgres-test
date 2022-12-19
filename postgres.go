package embeddedpostgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4/pgxpool"
)

func pgError(err error) (*pgconn.PgError, error) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return nil, errors.New("err was not a *pgconn.PgError")
	}
	return pgErr, nil
}

func pgErrorCode(err error) string {
	pgErr, err := pgError(err)
	if err != nil {
		return ""
	}
	return pgErr.Code
}

func connectionString(port uint32, username, password, database string) string {
	return fmt.Sprintf("host=localhost port=%v user=%v password=%v dbname=%v sslmode=disable", port, username, password, database)
}

func connect(ctx context.Context, port uint32, username, password, database string) (*pgxpool.Pool, error) {
	connStr := connectionString(port, username, password, database)
	return pgxpool.Connect(ctx, connStr)
}

func healthCheckDatabaseOrTimeout(ctx context.Context, config *config) error {
	ctx, cxl := context.WithTimeout(ctx, config.startTimeout)
	defer cxl()

	if healthCheck(ctx, config.port, config.username, config.password, config.database) == nil {
		return nil
	}

	pauseFor := time.Millisecond * 100
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("unhealthy after %v", config.startTimeout)
		case <-time.After(time.Millisecond):
			if healthCheck(ctx, config.port, config.username, config.password, config.database) == nil {
				return nil
			}
			if pauseFor > time.Millisecond*10 {
				pauseFor -= time.Millisecond * 10
			}
		}
	}
}

func healthCheck(ctx context.Context, port uint32, username, password, database string) (err error) {
	pool, err := connect(ctx, port, username, password, database)
	if err != nil {
		return err
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, "SELECT 1"); err != nil {
		return err
	}
	return nil
}
