package helpers

import (
	"errors"
	"testing"

	"github.com/lib/pq"
)

func TestIsSerialisationFailureErr(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "base",
			args: args{
				err: pq.Error{Code: _pqSerializationFailureCode},
			},
			want: true,
		},
		{
			name: "pointer",
			args: args{
				err: &pq.Error{Code: _pqSerializationFailureCode},
			},
			want: true,
		},
		{
			name: "other error",
			args: args{
				err: errors.New("other"),
			},
			want: false,
		},
		{
			name: "nil",
			args: args{
				err: nil,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsSerialisationFailureErr(tt.args.err); got != tt.want {
				t.Errorf("IsSerialisationFailureErr() = %v, want %v", got, tt.want)
			}
		})
	}
}
