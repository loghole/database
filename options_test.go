package database

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/loghole/database/hooks"
	"github.com/loghole/database/mocks"
	"github.com/stretchr/testify/assert"
)

func TestWithRetryPolicy(t *testing.T) {
	type fields struct {
		MaxAttempts       int
		InitialBackoff    time.Duration
		MaxBackoff        time.Duration
		BackoffMultiplier float64
		ErrIsRetryable    func(err error) bool
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			fields: fields{
				MaxAttempts:       DefaultRetryAttempts,
				InitialBackoff:    DefaultRetryInitialBackoff,
				MaxBackoff:        DefaultRetryMaxBackoff,
				BackoffMultiplier: DefaultRetryBackoffMultiplier,
				ErrIsRetryable: func(err error) bool {
					return false
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "invalid MaxAttempts",
			fields: fields{
				MaxAttempts:       0,
				InitialBackoff:    DefaultRetryInitialBackoff,
				MaxBackoff:        DefaultRetryMaxBackoff,
				BackoffMultiplier: DefaultRetryBackoffMultiplier,
				ErrIsRetryable: func(err error) bool {
					return false
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid InitialBackoff",
			fields: fields{
				MaxAttempts:       DefaultRetryAttempts,
				InitialBackoff:    -1,
				MaxBackoff:        DefaultRetryMaxBackoff,
				BackoffMultiplier: DefaultRetryBackoffMultiplier,
				ErrIsRetryable: func(err error) bool {
					return false
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid MaxBackoff",
			fields: fields{
				MaxAttempts:       DefaultRetryAttempts,
				InitialBackoff:    DefaultRetryInitialBackoff,
				MaxBackoff:        -1,
				BackoffMultiplier: DefaultRetryBackoffMultiplier,
				ErrIsRetryable: func(err error) bool {
					return false
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid BackoffMultiplier",
			fields: fields{
				MaxAttempts:       DefaultRetryAttempts,
				InitialBackoff:    DefaultRetryInitialBackoff,
				MaxBackoff:        DefaultRetryMaxBackoff,
				BackoffMultiplier: -1,
				ErrIsRetryable: func(err error) bool {
					return false
				},
			},
			wantErr: assert.Error,
		},
		{
			name: "invalid ErrIsRetryable",
			fields: fields{
				MaxAttempts:       DefaultRetryAttempts,
				InitialBackoff:    DefaultRetryInitialBackoff,
				MaxBackoff:        DefaultRetryMaxBackoff,
				BackoffMultiplier: DefaultRetryBackoffMultiplier,
				ErrIsRetryable:    nil,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts options

			err := opts.apply(&hooks.Config{}, WithRetryPolicy(RetryPolicy{
				MaxAttempts:       tt.fields.MaxAttempts,
				InitialBackoff:    tt.fields.InitialBackoff,
				MaxBackoff:        tt.fields.MaxBackoff,
				BackoffMultiplier: tt.fields.BackoffMultiplier,
				ErrIsRetryable:    tt.fields.ErrIsRetryable,
			}))

			tt.wantErr(t, err, fmt.Sprintf("validate()"))
		})
	}
}

func TestWithMetricsHook(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	type args struct {
		collector hooks.MetricCollector
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "pass",
			args: args{
				collector: mocks.NewMockMetricCollector(ctrl),
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var opts options

			err := opts.apply(&hooks.Config{}, WithMetricsHook(tt.args.collector))

			tt.wantErr(t, err, "metrics hook")
		})
	}
}
