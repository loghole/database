package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"

	"github.com/loghole/database"
	"github.com/loghole/database/internal/helpers"
)

func main() {
	db, err := database.New(&database.Config{
		Addr:     "postgres:5432",
		User:     "postgres:password",
		Database: "postgres",
		Type:     database.PostgresDatabase,
	}, database.WithCockroachRetryFunc())
	if err != nil {
		panic(err)
	}

	initDatabase(db)

	testRetry(db)
}

func testRetry(db *database.DB) {
	var wg sync.WaitGroup

	const query = `UPDATE test SET name=$1 WHERE id = $2`

	wg.Add(2)

	log.Println("start")

	go func() {
		for {
			if _, err := db.ExecContext(context.Background(), query, strconv.Itoa(rand.Int()), 1); err != nil {
				log.Printf("update without tx: retryable=%v, err=%v", helpers.IsSerialisationFailureErr(err), err)
			}

			// err := db.RunTxx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
			// 	if _, err := tx.Exec(query, strconv.Itoa(rand.Int()), 1); err != nil {
			// 		return err
			// 	}
			//
			// 	return nil
			// })
			// if err != nil {
			// 	log.Println("update without tx: ", err)
			// }

			time.Sleep(time.Millisecond * 100)
		}
	}()

	go func() {
		for {
			log.Println("start slow update")

			err := db.RunTxx(context.Background(), func(ctx context.Context, tx *sqlx.Tx) error {
				var id int64

				if err := tx.GetContext(ctx, &id, `SELECT id FROM test WHERE id=1 FOR UPDATE`); err != nil {
					return err
				}

				time.Sleep(time.Second * 3)

				if _, err := tx.Exec(query, strconv.Itoa(rand.Int()), 1); err != nil {
					return err
				}

				return nil
			})
			if err != nil {
				log.Println("slow update failed: ", err)
			}
		}
	}()

	wg.Wait()
}

func initDatabase(db *database.DB) {
	if _, err := db.ExecContext(context.TODO(), `CREATE TABLE IF NOT EXISTS test(
		id INTEGER NOT NULL,
		name TEXT NOT NULL
	)`); err != nil {
		panic(err)
	}

	if _, err := db.ExecContext(context.TODO(), `INSERT INTO test (id, name) VALUES (1, '1')`); err != nil {
		panic(err)
	}
	if _, err := db.ExecContext(context.TODO(), `INSERT INTO test (id, name) VALUES (2, '2')`); err != nil {
		panic(err)
	}
	if _, err := db.ExecContext(context.TODO(), `ALTER DATABASE postgres SET DEFAULT_TRANSACTION_ISOLATION TO SERIALIZABLE`); err != nil {
		panic(err)
	}
}
