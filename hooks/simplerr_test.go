package hooks

import (
	"context"
	"errors"
	"testing"

	"github.com/lissteron/simplerr"
	"github.com/loghole/dbhook"
	"github.com/stretchr/testify/assert"
)

func TestSimplerrHook_Error(t *testing.T) {
	type args struct {
		ctx   context.Context
		input *dbhook.HookInput
	}
	tests := []struct {
		name    string
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no error",
			args: args{
				ctx: context.Background(),
				input: &dbhook.HookInput{
					Query:  "SELECT 1",
					Caller: dbhook.CallerQuery,
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "simplerr error",
			args: args{
				ctx: context.Background(),
				input: &dbhook.HookInput{
					Query:  "SELECT 1",
					Error:  simplerr.NewWithCode("qwerty", simplerr.InternalCode(1)),
					Caller: dbhook.CallerQuery,
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Equal(t, simplerr.GetCode(err).Int(), 1)

				return true
			},
		},
		{
			name: "random error",
			args: args{
				ctx: context.Background(),
				input: &dbhook.HookInput{
					Query:  "SELECT 1",
					Error:  errors.New("random"),
					Caller: dbhook.CallerQuery,
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Equal(t, simplerr.GetCode(err).Int(), int(DatabaseError))

				return true
			},
		},
		{
			name: "reconnect error",
			args: args{
				ctx: context.Background(),
				input: &dbhook.HookInput{
					Query:  "SELECT 1",
					Error:  ErrCanRetry,
					Caller: dbhook.CallerQuery,
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Equal(t, simplerr.GetCode(err).Int(), int(Reconnected))

				return true
			},
		},
		{
			name: "bad connection error",
			args: args{
				ctx: context.Background(),
				input: &dbhook.HookInput{
					Query:  "SELECT 1",
					Error:  errors.New("server is not accepting clients"),
					Caller: dbhook.CallerQuery,
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Equal(t, simplerr.GetCode(err).Int(), int(BadConnection))

				return true
			},
		},
		{
			name: "bad connection error",
			args: args{
				ctx: context.Background(),
				input: &dbhook.HookInput{
					Query:  "SELECT 1",
					Error:  errors.New("connection refused"),
					Caller: dbhook.CallerQuery,
				},
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				assert.Equal(t, simplerr.GetCode(err).Int(), int(BadConnection))

				return true
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewSimplerrHook()

			_, err := h.Error(tt.args.ctx, tt.args.input)
			if !tt.wantErr(t, err, "Error() error = %v, wantErr %v", err, tt.wantErr) {
				return
			}
		})
	}
}
