package database

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	type args struct {
		cfg  *Config
		opts []Option
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				cfg: &Config{
					Database: ":memory:",
					Type:     SQLiteDatabase,
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid driver type",
			args: args{
				cfg: &Config{
					Database: ":memory:",
					Type:     "qwerty",
				},
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDb, err := New(tt.args.cfg, tt.args.opts...)
			if !tt.wantErr(t, err, fmt.Sprintf("New(%v, %v)", tt.args.cfg, tt.args.opts)) {
				return
			}

			if err != nil {
				return
			}

			assert.NotNil(t, gotDb, "New(%v, %v)", tt.args.cfg, tt.args.opts)
		})
	}
}

func TestDB_PingContext(t *testing.T) {
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				ctx: context.Background(),
			},
			wantErr: assert.NoError,
		},
		{
			name: "ctx error",
			args: args{
				ctx: contextCanceled(),
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t)

			tt.wantErr(t, db.PingContext(tt.args.ctx), fmt.Sprintf("PingContext(%v)", tt.args.ctx))
		})
	}
}

func TestDB_SetConnMaxIdleTime(t *testing.T) {
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "pass",
			args: args{
				time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t)

			db.SetConnMaxIdleTime(tt.args.d)
		})
	}
}

func TestDB_SetMaxIdleConns(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "pass",
			args: args{
				n: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t)

			db.SetMaxIdleConns(tt.args.n)
		})
	}
}

func TestDB_SetMaxOpenConns(t *testing.T) {
	type args struct {
		n int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "pass",
			args: args{
				n: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := memorySQLLite(t)

			db.SetMaxOpenConns(tt.args.n)
		})
	}
}

func contextCanceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}
