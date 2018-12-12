package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
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

	var spannerWriteSession float64
	{
		spannerWriteSessionParam := os.Getenv("SPANNER_WRITE_SESSION")
		fmt.Printf("Env spannerWriteSession:%s\n", spannerWriteSessionParam)
		v, err := strconv.ParseFloat(spannerWriteSessionParam, 64)
		if err != nil {
			panic(err)
		}
		spannerWriteSession = v
	}

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
	client := CreateClient(ctx, spannerDatabase, spannerMinOpened, spannerWriteSession)

	ts := TweetStore{
		sc: client,
	}

	tl, err := ts.QueryRandomSampling(ctx)
	if err != nil {
		panic(err)
	}

	const endCount = 90
	const intervalMinute = 33
	errCh := make(chan error)
	go func() {
		for i := 0; i < endCount; i++ {
			ctx := context.Background()

			if err := ts.Update(ctx, tl[rand.Intn(99)].ID); err != nil {
				log.Printf("failed spanner.Update. err = %+v\n", err)
				errCh <- err
			} else {
				log.Println("success spanner.Update.")

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
				errCh <- err
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
