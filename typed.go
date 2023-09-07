package transactional

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
)

// WithDBAndType() creates a new Typed using the given db and type.
func WithDBAndType[T any](db *sqlx.DB) *Typed[T] {
	return &Typed[T]{dbx: db}
}

// Typed can wrap functions into transactions.
type Typed[T any] struct {
	dbx *sqlx.DB
}

// WrapFn wraps a function within a transaction.
func (t *Typed[T]) WrapFn(
	ctx context.Context,
	fn func(tx *sqlx.Tx) (T, error),
) (T, error) {
	var r T
	tx, err := t.dbx.BeginTxx(ctx, nil)
	if err != nil {
		return r, fmt.Errorf("failed to begin transaction: %w", err)
	}

	r, err = fn(tx)
	if err != nil {
		return r, rollback(tx, err)
	}

	if err := tx.Commit(); err != nil {
		if errors.Is(err, sql.ErrTxDone) {
			// this is ok because whoever did finish the tx should have also written the error already.
			return r, nil
		}
		return r, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return r, nil
}
