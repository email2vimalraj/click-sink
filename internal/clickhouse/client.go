package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/yourname/click-sink/internal/config"
)

type Client struct {
	conn ch.Conn
	cfg  *config.ClickHouseConfig
}

func NewClient(cfg *config.ClickHouseConfig) (*Client, error) {
	opts, err := parseDSN(cfg.DSN)
	if err != nil {
		return nil, err
	}
	conn, err := ch.Open(opts)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, cfg: cfg}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) EnsureTable(ctx context.Context, table string, columns []Column) error {
	if !c.cfg.CreateTableIfMissing {
		return nil
	}
	cols := make([]string, 0, len(columns))
	for _, col := range columns {
		cols = append(cols, fmt.Sprintf("`%s` %s", col.Name, col.Type))
	}
	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s) ENGINE = MergeTree ORDER BY tuple()", c.cfg.Database, table, strings.Join(cols, ", "))
	return c.conn.Exec(ctx, ddl)
}

type Column struct {
	Name string
	Type string
}

type Row []any

func (c *Client) InsertBatch(ctx context.Context, table string, columns []Column, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	batch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s.%s (%s)", c.cfg.Database, table, strings.Join(names, ",")))
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := batch.Append(r...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func parseDSN(dsn string) (*ch.Options, error) {
	// Support native clickhouse://username:password@host:9000?database=default
	// Also support http(s):// with ClickHouse HTTP interface.
	// For simplicity, use ch.ParseDSN if available; otherwise manual parse.
	opts, err := ch.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	if opts.Auth.Database == "" {
		return nil, errors.New("dsn must include database")
	}
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}
	return opts, nil
}
