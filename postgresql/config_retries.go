package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/blang/semver"
	"github.com/hashicorp/terraform-plugin-log/tflog"
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
	return retryWithData(func() (*sql.Rows, error) {
		return db.QueryContext(context.Background(), query, args...)
	})
}

func (db *RetryableDBConnection) Begin() (*sql.Tx, error) {
	return retryWithData(func() (*sql.Tx, error) {
		return db.BeginTx(context.Background(), nil)
	})
}

const maxRetries = 5

func retryWithData[T any](operation func() (T, error)) (data T, err error) {
	for i := 0; i < maxRetries; i++ {
		data, err = operation()
		if err == nil {
			return
		}

		tflog.Warn(context.Background(), fmt.Sprintf("[attempt %d/%d] Retried operation with data", i+1, maxRetries))
	}

	return
}

func retry(operation func() error) (err error) {
	for i := 0; i < maxRetries; i++ {
		err = operation()
		if err == nil {
			return
		}

		tflog.Warn(context.Background(), fmt.Sprintf("[attempt %d/%d] Retried operation", i+1, maxRetries))
	}

	return
}
