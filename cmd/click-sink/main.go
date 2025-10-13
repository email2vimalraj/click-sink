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
	"github.com/yourname/click-sink/internal/store"
	"github.com/yourname/click-sink/internal/ui"
	"github.com/yourname/click-sink/internal/worker"
)

type CLI struct {
	Config  string `help:"Path to pipeline config YAML" short:"c" type:"existingfile"`
	Mapping string `help:"Path to field mapping YAML (generated or edited)" short:"m" type:"existingfile" optional:""`

	Detect struct {
		Sample  int    `help:"Number of messages to sample for schema detection" default:"200"`
		Timeout string `help:"Max time to wait for samples (e.g. 10s, 1m)" default:"15s"`
	} `cmd:"" help:"Detect schema from Kafka and print a recommended mapping YAML to stdout"`

	Run struct{} `cmd:"" help:"Run the Kafka→ClickHouse sink pipeline"`

	UI struct {
		Listen string `help:"Listen address for the UI server" default:":8081"`
		Data   string `help:"Data directory to store config and mapping" default:".ui-data"`
		Sample int    `help:"Sample size for detection" default:"100"`
		Store  string `help:"Persistence backend: fs or pg" enum:"fs,pg" default:"fs"`
		PgDSN  string `help:"Postgres DSN when --store=pg (e.g. postgres://user:pass@host:5432/db?sslmode=disable)" default:""`
	} `cmd:"" help:"Launch web UI for configuring and running pipelines"`

	Worker struct {
		Data     string `help:"Data directory for store (when --store=fs)" default:".ui-data"`
		Interval string `help:"Reconcile interval (e.g. 5s, 10s)" default:"5s"`
		LeaseTTL string `help:"Lease TTL (e.g. 20s)" default:"20s"`
		WorkerID string `help:"Override worker id (defaults to hostname:PORT)" default:""`
		Store    string `help:"Persistence backend: fs or pg" enum:"fs,pg" default:"fs"`
		PgDSN    string `help:"Postgres DSN when --store=pg (e.g. postgres://user:pass@host:5432/db?sslmode=disable)" default:""`
	} `cmd:"" help:"Run background worker to reconcile desired state and execute pipelines"`
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli, kong.Name("click-sink"), kong.Description("Kafka → ClickHouse sink"))

	switch ctx.Command() {
	case "detect":
		if cli.Config == "" {
			log.Fatalf("--config is required for detect")
		}
		cfg, err := config.Load(cli.Config)
		if err != nil {
			log.Fatalf("load config: %v", err)
		}
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
		if cli.Config == "" {
			log.Fatalf("--config is required for run")
		}
		if cli.Mapping == "" {
			log.Fatalf("--mapping is required for run")
		}
		cfg, err := config.Load(cli.Config)
		if err != nil {
			log.Fatalf("load config: %v", err)
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
	case "ui":
		// Launch UI server with pluggable store backend
		var srv *ui.Server
		var err error
		switch cli.UI.Store {
		case "fs":
			tfs := os.DirFS("internal/ui/templates")
			srv, err = ui.New(cli.UI.Listen, cli.UI.Data, cli.UI.Sample, tfs)
		case "pg":
			if cli.UI.PgDSN == "" {
				log.Fatalf("--pg-dsn is required when --store=pg")
			}
			st, err2 := store.NewPGStore(cli.UI.PgDSN)
			if err2 != nil {
				log.Fatalf("pg store: %v", err2)
			}
			srv, err = ui.NewWithStore(cli.UI.Listen, cli.UI.Sample, st)
		default:
			log.Fatalf("unknown store backend: %s", cli.UI.Store)
		}
		if err != nil {
			log.Fatalf("ui: %v", err)
		}
		log.Printf("UI listening on %s (store=%s)", cli.UI.Listen, cli.UI.Store)
		if err := srv.Start(); err != nil {
			log.Fatalf("ui: %v", err)
		}
	case "worker":
		var st store.PipelineStore
		switch cli.Worker.Store {
		case "fs":
			st = store.NewFSStore(cli.Worker.Data)
		case "pg":
			if cli.Worker.PgDSN == "" {
				log.Fatalf("--pg-dsn is required when --store=pg")
			}
			p, err := store.NewPGStore(cli.Worker.PgDSN)
			if err != nil {
				log.Fatalf("pg store: %v", err)
			}
			st = p
		default:
			log.Fatalf("unknown store backend: %s", cli.Worker.Store)
		}
		recon := 5 * time.Second
		if d, err := time.ParseDuration(cli.Worker.Interval); err == nil {
			recon = d
		}
		lease := 20 * time.Second
		if d, err := time.ParseDuration(cli.Worker.LeaseTTL); err == nil {
			lease = d
		}
		wid := cli.Worker.WorkerID
		if wid == "" {
			wid = worker.DefaultWorkerID()
		}
		r := worker.NewRunner(st, wid, recon, lease)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			c := make(chan os.Signal, 1)
			signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
			<-c
			cancel()
		}()
		if err := r.Run(ctx); err != nil && err != context.Canceled {
			log.Fatalf("worker: %v", err)
		}
	default:
		time.Sleep(0)
	}
}
