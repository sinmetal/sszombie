package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/spanner"
)

func CreateClient(ctx context.Context, db string, spannerMinOpened uint64) *spanner.Client {
	ctx, span := startSpan(ctx, "createClient")
	defer span.End()

	o := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			MinOpened:           spannerMinOpened,
			HealthCheckInterval: 24 * time.Hour,
		},
	}
	dataClient, err := spanner.NewClientWithConfig(ctx, db, o)
	if err != nil {
		log.Fatal(err)
	}

	return dataClient
}
