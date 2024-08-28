// Package transactional provides sqlx transactional wrappers for http handlers and functions.
package transactional

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"

	"github.com/caarlos0/httperr/v2"
	"github.com/jmoiron/sqlx"
)

// WithDB creates a new transactional using the given db.
func WithDB(db *sqlx.DB) *Transactional {
	return &Transactional{dbx: db}
}

// Transactional can wrap functions and http handlers within a transaction.
type Transactional struct {
	dbx *sqlx.DB
}

// WrapF wraps the given http handler func within a transaction.
func (t *Transactional) WrapF(h HandlerFunc, eh httperr.ErrorHandler) http.Handler {
	return t.Wrap(Handler(h), eh)
}

// Wrap wraps the given http handler within a transaction.
func (t *Transactional) Wrap(h Handler, eh httperr.ErrorHandler) http.Handler {
	return httperr.NewFWithHandler(func(w http.ResponseWriter, r *http.Request) error {
		ctx := r.Context()
		ptx := ctx.Value(txKey{})
		if ptx != nil {
			tx := ptx.(*sqlx.Tx)
			if err := h.ServeHTTP(tx, w, r); err != nil {
				return rollback(tx, err)
			}
			return nil
		}

		return t.WrapFn(ctx, func(tx *sqlx.Tx) error {
			ctx = context.WithValue(ctx, txKey{}, tx)
			return h.ServeHTTP(tx, w, r.WithContext(ctx))
		})
	}, eh)
}

// WrapFn wraps a function within a transaction.
func (t *Transactional) WrapFn(ctx context.Context, fn func(tx *sqlx.Tx) error) error {
	tx, err := t.dbx.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	if err := fn(tx); err != nil {
		return rollback(tx, err)
	}

	if err := tx.Commit(); err != nil {
		if errors.Is(err, sql.ErrTxDone) {
			// this is ok because whoever did finish the tx should have also written the error already.
			return nil
		}
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

type txKey struct{}

func rollback(tx *sqlx.Tx, err error) error {
	if rerr := tx.Rollback(); rerr != nil {
		if errors.Is(rerr, sql.ErrTxDone) {
			return err
		}
		return fmt.Errorf("failed to rollback: %s: %w", err.Error(), rerr)
	}

	if errors.Is(err, sql.ErrNoRows) {
		return httperr.Wrap(err, http.StatusNotFound)
	}
	return err
}

// Handler is a HTTP handler that takes a transaction and might error.
type Handler interface {
	ServeHTTP(tx *sqlx.Tx, w http.ResponseWriter, r *http.Request) error
}

// HandlerFunc is Handler function.
type HandlerFunc func(tx *sqlx.Tx, w http.ResponseWriter, r *http.Request) error

// ServeHTTP satisfies Handler.
func (f HandlerFunc) ServeHTTP(tx *sqlx.Tx, w http.ResponseWriter, r *http.Request) error {
	return f(tx, w, r)
}
