package database

import (
	"fmt"
	"testing"

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
