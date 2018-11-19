package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/spanner"
	"contrib.go.opencensus.io/exporter/stackdriver"
	"go.opencensus.io/trace"
)

func main() {
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

	for {
		ctx := context.Background()
		if err := query(ctx, client); err != nil {
			log.Printf("failed spanner.Query. err = %+v\n", err)
		} else {
			log.Println("success spanner.Query.")

		}
		time.Sleep(90 * time.Minute)
	}
}

func query(ctx context.Context, client *spanner.Client) error {
	ctx, span := startSpan(ctx, "query")
	defer span.End()

	return client.Single().Query(ctx, spanner.NewStatement("SELECT 1")).Do(func(r *spanner.Row) error {
		return nil
	})
}

func startSpan(ctx context.Context, name string) (context.Context, *trace.Span) {
	return trace.StartSpan(ctx, fmt.Sprintf("/sszombie/%s", name))
}
