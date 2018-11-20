package main

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type Tweet struct {
	ID         string `spanner:"Id"`
	SearchID   string `spanner:"SearchId"`
	CreatedAt  time.Time
	CommitedAt time.Time
}

type TweetStore struct {
	sc *spanner.Client
}

func (s *TweetStore) Insert(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "insert")
	defer span.End()

	now := time.Now()

	ml := []*spanner.Mutation{}
	for i := 0; i < 3; i++ {
		t := Tweet{
			ID:         id,
			CreatedAt:  now,
			CommitedAt: spanner.CommitTimestamp,
		}
		m, err := spanner.InsertStruct(fmt.Sprintf("Tweet%d", i), t)
		if err != nil {
			return err
		}
		ml = append(ml, m)
	}

	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		return txn.BufferWrite(ml)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *TweetStore) QueryRandomSampling(ctx context.Context) error {
	ctx, span := startSpan(ctx, "queryRandomSampling")
	defer span.End()

	sql := `SELECT * FROM Tweet0 TABLESAMPLE RESERVOIR (10 ROWS);`
	iter := s.sc.Single().Query(ctx, spanner.Statement{SQL: sql})
	defer iter.Stop()

	t := Tweet{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		if err := row.ToStruct(&t); err != nil {
			return err
		}
	}

	return nil
}
