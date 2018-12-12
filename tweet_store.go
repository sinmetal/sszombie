package main

import (
	"context"
	"fmt"
	"log"
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

func (s *TweetStore) Update(ctx context.Context, id string) error {
	ctx, span := startSpan(ctx, "update")
	defer span.End()

	log.Printf("Tweet ID : %s\n", id)
	_, err := s.sc.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ml := []*spanner.Mutation{}
		row, err := txn.ReadRow(ctx, "Tweet0", spanner.Key{id}, []string{"Id", "SearchId", "CreatedAt", "CommitedAt"})
		if err != nil {
			return err
		} else {
			t := Tweet{}
			if err := row.ToStruct(&t); err != nil {
				return err
			}

			m, err := spanner.UpdateStruct("Tweet0", &t)
			if err != nil {
				return err
			}
			ml = append(ml, m)
		}

		return txn.BufferWrite(ml)
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *TweetStore) QueryRandomSampling(ctx context.Context) ([]*Tweet, error) {
	ctx, span := startSpan(ctx, "queryRandomSampling")
	defer span.End()

	sql := `SELECT * FROM Tweet0 TABLESAMPLE RESERVOIR (100 ROWS);`
	iter := s.sc.Single().Query(ctx, spanner.Statement{SQL: sql})
	defer iter.Stop()

	tl := []*Tweet{}
	t := Tweet{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return tl, err
		}
		if err := row.ToStruct(&t); err != nil {
			return tl, err
		}
		tl = append(tl, &t)
	}

	return tl, nil
}
