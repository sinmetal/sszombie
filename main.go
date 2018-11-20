package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"contrib.go.opencensus.io/exporter/stackdriver"
	"github.com/google/uuid"
	"go.opencensus.io/trace"
)

func main() {
	log.Println("sszombie is ignite")

	projectID, err := GetProjectID()
	if err != nil {
		panic(err)
	}
	spannerDatabase := os.Getenv("SPANNER_DATABASE")
	fmt.Printf("Env SPANNER_DATABASE:%s\n", spannerDatabase)

	spannerMinOpenedParam := os.Getenv("SPANNER_MIN_OPENED")
	fmt.Printf("Env spannerMinOpened:%s\n", spannerMinOpenedParam)
	v, err := strconv.Atoi(spannerMinOpenedParam)
	if err != nil {
		panic(err)
	}
	spannerMinOpened := uint64(v)

	// Create and register a OpenCensus Stackdriver Trace exporter.
	exporter, err := stackdriver.NewExporter(stackdriver.Options{
		ProjectID: projectID,
	})
	if err != nil {
		log.Fatal(err)
	}
	trace.RegisterExporter(exporter)

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()}) // defaultでは10,000回に1回のサンプリングになっているが、リクエストが少ないと出てこないので、とりあえず全部出す

	ctx := context.Background()
	client := CreateClient(ctx, spannerDatabase, spannerMinOpened)

	ts := TweetStore{
		sc: client,
	}

	const endCount = 90
	const intervalMinute = 190
	errCh := make(chan error)
	go func() {
		for i := 0; i < endCount; i++ {
			ctx := context.Background()

			if err := ts.QueryRandomSampling(ctx); err != nil {
				log.Printf("failed spanner.Query. err = %+v\n", err)
			} else {
				log.Println("success spanner.Query.")

			}
			time.Sleep(intervalMinute * time.Minute)
		}
		errCh <- nil
	}()

	go func() {
		for i := 0; i < endCount; i++ {
			ctx := context.Background()

			id := uuid.New().String()
			if err := ts.Insert(ctx, id); err != nil {
				log.Printf("failed spanner.Insert. err = %+v\n", err)
			} else {
				log.Printf("success spanner.Insert. id = %+v\n", id)
			}
			time.Sleep(intervalMinute * time.Minute)
		}

		errCh <- nil
	}()

	err = <-errCh
	if err != nil {
		log.Fatalf("failed err = %+v", err)
	}
	log.Println("sszombie is done")
}

func startSpan(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, fmt.Sprintf("/sszombie/%s", name))
}
