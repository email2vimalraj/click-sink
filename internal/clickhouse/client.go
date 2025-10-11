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
	// If cfg.Database is not set but DSN had a database, propagate it so helper methods have it.
	if cfg.Database == "" && opts != nil && opts.Auth.Database != "" {
		cfg.Database = opts.Auth.Database
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
		cols = append(cols, fmt.Sprintf("`%s` %s", escapeIdent(col.Name), col.Type))
	}
	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) ENGINE = MergeTree ORDER BY tuple()",
		escapeIdent(c.cfg.Database), escapeIdent(table), strings.Join(cols, ", "),
	)
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
		names[i] = fmt.Sprintf("`%s`", escapeIdent(col.Name))
	}
	stmt := fmt.Sprintf(
		"INSERT INTO `%s`.`%s` (%s)",
		escapeIdent(c.cfg.Database), escapeIdent(table), strings.Join(names, ","),
	)
	batch, err := c.conn.PrepareBatch(ctx, stmt)
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

// Ping validates the connection by issuing a ping to ClickHouse.
func (c *Client) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// TableExists checks if a table exists in the configured or provided database.
func (c *Client) TableExists(ctx context.Context, db, table string) (bool, error) {
	if db == "" {
		db = c.cfg.Database
	}
	if table == "" {
		return false, errors.New("table name required")
	}
	// Use EXISTS TABLE which returns 1 or 0
	var exists uint8
	q := fmt.Sprintf("EXISTS TABLE `%s`.`%s`", escapeIdent(db), escapeIdent(table))
	if err := c.conn.QueryRow(ctx, q).Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, nil
}

// CreateTable creates a MergeTree table with the provided columns regardless of cfg.CreateTableIfMissing.
func (c *Client) CreateTable(ctx context.Context, db, table string, columns []Column) error {
	if db == "" {
		db = c.cfg.Database
	}
	if table == "" {
		return errors.New("table name required")
	}
	if len(columns) == 0 {
		return errors.New("at least one column is required to create table")
	}
	cols := make([]string, 0, len(columns))
	for _, col := range columns {
		cols = append(cols, fmt.Sprintf("`%s` %s", escapeIdent(col.Name), col.Type))
	}
	ddl := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s) ENGINE = MergeTree ORDER BY tuple()",
		escapeIdent(db), escapeIdent(table), strings.Join(cols, ", "),
	)
	return c.conn.Exec(ctx, ddl)
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

// escapeIdent escapes backticks in identifiers to safely wrap them with backticks.
func escapeIdent(s string) string {
	return strings.ReplaceAll(s, "`", "``")
}

// ListDatabases returns database names
func (c *Client) ListDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SELECT name FROM system.databases ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// ListTables returns table names in a database
func (c *Client) ListTables(ctx context.Context, db string) ([]string, error) {
	if db == "" {
		db = c.cfg.Database
	}
	rows, err := c.conn.Query(ctx, "SELECT name FROM system.tables WHERE database = ? ORDER BY name", db)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// GetTableSchema returns columns (name,type) for a table in a database
func (c *Client) GetTableSchema(ctx context.Context, db, table string) ([]Column, error) {
	if db == "" {
		db = c.cfg.Database
	}
	if table == "" {
		return nil, errors.New("table name required")
	}
	rows, err := c.conn.Query(ctx, "SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position", db, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Column
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out = append(out, Column{Name: name, Type: typ})
	}
	return out, rows.Err()
}
