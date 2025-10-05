package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/alecthomas/kong"

	"github.com/yourname/click-sink/internal/config"
	"github.com/yourname/click-sink/internal/pipeline"
	"github.com/yourname/click-sink/internal/schema"
)

type CLI struct {
	Config  string `help:"Path to pipeline config YAML" required:"" short:"c" type:"existingfile"`
	Mapping string `help:"Path to field mapping YAML (generated or edited)" short:"m" type:"existingfile" optional:""`

	Detect struct {
		Sample  int    `help:"Number of messages to sample for schema detection" default:"200"`
		Timeout string `help:"Max time to wait for samples (e.g. 10s, 1m)" default:"15s"`
	} `cmd:"" help:"Detect schema from Kafka and print a recommended mapping YAML to stdout"`

	Run struct{} `cmd:"" help:"Run the Kafka→ClickHouse sink pipeline"`
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli, kong.Name("click-sink"), kong.Description("Kafka → ClickHouse sink"))

	cfg, err := config.Load(cli.Config)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	switch ctx.Command() {
	case "detect":
		d := context.Background()
		if cli.Detect.Timeout != "" {
			if dur, err := time.ParseDuration(cli.Detect.Timeout); err == nil {
				var cancel context.CancelFunc
				d, cancel = context.WithTimeout(context.Background(), dur)
				defer cancel()
			}
		}
		mapping, err := schema.DetectAndRecommend(d, cfg, cli.Detect.Sample)
		if err != nil {
			log.Fatalf("detect schema: %v", err)
		}
		if len(mapping) == 0 {
			fmt.Fprintln(os.Stderr, "No messages sampled; try increasing --sample, timeout, or ensure topic has data")
		}
		fmt.Println(string(mapping))
	case "run":
		if cli.Mapping == "" {
			log.Fatalf("--mapping is required for run")
		}
		m, err := os.ReadFile(cli.Mapping)
		if err != nil {
			log.Fatalf("read mapping: %v", err)
		}
		mp, err := schema.ParseMapping(m)
		if err != nil {
			log.Fatalf("parse mapping: %v", err)
		}
		p, err := pipeline.New(cfg, mp)
		if err != nil {
			log.Fatalf("init pipeline: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// Handle SIGINT/SIGTERM for graceful shutdown
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			<-c
			cancel()
		}()
		if err := p.Run(ctx); err != nil {
			log.Fatalf("pipeline error: %v", err)
		}
	default:
		time.Sleep(0)
	}
}
