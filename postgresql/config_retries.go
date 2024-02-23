package postgresql

import (
	"context"
	"database/sql"
	"github.com/blang/semver"
)

type RetryableDBConnection struct {
	*DBConnection
}

func NewRetryableConnection(db *DBConnection) *RetryableDBConnection {
	return &RetryableDBConnection{db}
}

func (db *RetryableDBConnection) GetClient() *Client {
	return db.client
}

func (db *RetryableDBConnection) GetVersion() semver.Version {
	return db.version
}

func (db *RetryableDBConnection) FeatureSupported(name featureName) bool {
	return db.featureSupported(name)
}

func (db *RetryableDBConnection) IsSuperuser() (bool, error) {
	return db.isSuperuser()
}

func (db *RetryableDBConnection) Query(query string, args ...any) (*sql.Rows, error) {
	return retryOperation(func() (*sql.Rows, error) {
		return db.QueryContext(context.Background(), query, args...)
	})
}

func (db *RetryableDBConnection) Begin() (*sql.Tx, error) {
	return retryOperation(func() (*sql.Tx, error) {
		return db.BeginTx(context.Background(), nil)
	})
}

func retryOperation[T any](operation func() (T, error)) (result T, err error) {
	maxRetries := 5

	for i := 0; i < maxRetries; i++ {
		result, err = operation()
		if err == nil {
			return
		}
	}

	return
}
