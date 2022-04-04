package query

import (
	"bufio"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser_ParseCached(t *testing.T) {
	p := NewParser()

	want := Operation{
		Type:  SelectType,
		Table: "users",
	}

	got := p.Parse("SELECT id, name, phone FROM users WHERE id=$1")
	assert.Equal(t, want, got)

	got = p.Parse("SELECT id, name, phone FROM users WHERE id=$1")
	assert.Equal(t, want, got)
}

func TestParser_Parse(t *testing.T) {
	type args struct {
		stmt string
	}
	tests := []struct {
		name string
		args args
		want Operation
	}{
		{
			name: "select 1",
			args: args{
				stmt: "SELECT id, name, phone FROM users WHERE id=$1",
			},
			want: Operation{
				Type:  SelectType,
				Table: "users",
			},
		},
		{
			name: "select 2",
			args: args{
				stmt: "SELECT 1",
			},
			want: Operation{
				Type:  SelectType,
				Table: "unknown",
			},
		},
		{
			name: "insert 1",
			args: args{
				stmt: "INSERT INTO users (name, phone) VALUES ($1, $2)",
			},
			want: Operation{
				Type:  InsertType,
				Table: "users",
			},
		},
		{
			name: "insert 2",
			args: args{
				stmt: "INSERT INTO test SELECT * FROM boo",
			},
			want: Operation{
				Type:  InsertType,
				Table: "test",
			},
		},
		{
			name: "upsert 1",
			args: args{
				stmt: "UPSERT INTO test (id, name, phone) VALUES ($1, $2, $3)",
			},
			want: Operation{
				Type:  UpsertType,
				Table: "test",
			},
		},
		{
			name: "update 1",
			args: args{
				stmt: "update test set a = 1",
			},
			want: Operation{
				Type:  UpdateType,
				Table: "test",
			},
		},
		{
			name: "call 1",
			args: args{
				stmt: "call procedure",
			},
			want: Operation{
				Type:  CallType,
				Table: "procedure",
			},
		},
		{
			name: "call 2",
			args: args{
				stmt: "call procedure(1, 2)",
			},
			want: Operation{
				Type:  CallType,
				Table: "procedure",
			},
		},
		{
			name: "exec 1",
			args: args{
				stmt: "exec procedure",
			},
			want: Operation{
				Type:  ExecType,
				Table: "procedure",
			},
		},
		{
			name: "execute 1",
			args: args{
				stmt: "execute procedure",
			},
			want: Operation{
				Type:  ExecuteType,
				Table: "procedure",
			},
		},
		{
			name: "unknown",
			args: args{
				stmt: "unknown from users",
			},
			want: Operation{
				Type:  "unknown",
				Table: "unknown",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser()

			if got := p.Parse(tt.args.stmt); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScanSQLToken(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "select",
			args: args{
				query: "SELECT id FROM users WHERE name=$1",
			},
			want: []string{"SELECT", "id", "FROM", "users", "WHERE", "name=$1"},
		},
		{
			name: "insert",
			args: args{
				query: "INSERT INTO users (id, name) VALUES ($1, $2)",
			},
			want: []string{"INSERT", "INTO", "users", "id", "name", "VALUES", "$1", "$2"},
		},
		{
			name: "insert short",
			args: args{
				query: "INSERT INTO users(id, name)VALUES($1, $2)",
			},
			want: []string{"INSERT", "INTO", "users", "id", "name", "VALUES", "$1", "$2"},
		},
		{
			name: "with insert",
			args: args{
				query: "WITH q1 AS(SELECT id, name FROM users)INSERT INTO users(id, name)VALUES(q1.id, q1.name) FROM q1",
			},
			want: []string{"WITH", "q1", "AS", "SELECT", "id", "name", "FROM", "users", "INSERT", "INTO", "users", "id", "name", "VALUES", "q1", "id", "q1", "name", "FROM", "q1"},
		},
		{
			name: "specific delimiters",
			args: args{
				query: "WITH" + string('\u0085') + "q1" + string('\u1680') + "AS(SELECT id, name FROM users)INSERT" + string('\u2000') + "INTO users(id, name)VALUES(q1.id, q1.name) FROM q1",
			},
			want: []string{"WITH", "q1", "AS", "SELECT", "id", "name", "FROM", "users", "INSERT", "INTO", "users", "id", "name", "VALUES", "q1", "id", "q1", "name", "FROM", "q1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scan := bufio.NewScanner(strings.NewReader(tt.args.query))
			scan.Split(scanSQLToken)

			var result []string

			for scan.Scan() {
				result = append(result, scan.Text())
			}

			if !assert.NoError(t, scan.Err()) {
				return
			}

			assert.Equalf(t, tt.want, result, "result not equal")
		})
	}
}
