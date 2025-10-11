package schema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/yourname/click-sink/internal/config"
	"github.com/yourname/click-sink/internal/kafka"
	"gopkg.in/yaml.v3"
)

type Mapping struct {
	Columns []MapColumn `yaml:"columns" json:"columns"`
}

type MapColumn struct {
	FieldPath string `yaml:"fieldPath" json:"fieldPath"` // JSONPath-lite using dot notation
	Column    string `yaml:"column" json:"column"`
	Type      string `yaml:"type" json:"type"` // ClickHouse type
	Nullable  bool   `yaml:"nullable" json:"nullable"`
}

func ParseMapping(b []byte) (*Mapping, error) {
	var m Mapping
	if err := yaml.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func (m *Mapping) ToYAML() ([]byte, error) {
	buf := &bytes.Buffer{}
	enc := yaml.NewEncoder(buf)
	enc.SetIndent(2)
	if err := enc.Encode(m); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DetectAndRecommend samples messages and emits a suggested mapping.
func DetectAndRecommend(ctx context.Context, cfg *config.Config, sample int) ([]byte, error) {
	payloads, err := kafka.Sample(ctx, &cfg.Kafka, sample)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			empty := Mapping{Columns: []MapColumn{}}
			return empty.ToYAML()
		}
		return nil, err
	}
	if len(payloads) == 0 {
		// Return an empty mapping document as a hint
		empty := Mapping{Columns: []MapColumn{}}
		return empty.ToYAML()
	}
	merged := make(map[string][]any)
	for _, p := range payloads {
		var v any
		if err := json.Unmarshal(p, &v); err != nil {
			continue
		}
		flat := flattenJSON("", v)
		for k, val := range flat {
			merged[k] = append(merged[k], val)
		}
	}
	columns := make([]MapColumn, 0, len(merged))
	for path, samples := range merged {
		recType, nullable := recommendType(samples)
		columns = append(columns, MapColumn{
			FieldPath: path,
			Column:    sanitizeColumnName(path),
			Type:      recType,
			Nullable:  nullable,
		})
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].Column < columns[j].Column })
	m := Mapping{Columns: columns}
	return m.ToYAML()
}

// Helpers below

func sanitizeColumnName(s string) string {
	res := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			res = append(res, r)
		} else {
			res = append(res, '_')
		}
	}
	return string(res)
}

func flattenJSON(prefix string, v any) map[string]any {
	out := map[string]any{}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			for kk, vv := range flattenJSON(key, val) {
				out[kk] = vv
			}
		}
	case []any:
		// store arrays as JSON strings for now
		b, _ := json.Marshal(t)
		out[prefix] = string(b)
	default:
		out[prefix] = t
	}
	return out
}

func recommendType(samples []any) (string, bool) {
	nullable := false
	isInt := true
	isFloat := true
	isBool := true
	for _, s := range samples {
		if s == nil {
			nullable = true
			continue
		}
		switch v := s.(type) {
		case float64:
			// JSON numbers are float64; detect integer-like
			if v != float64(int64(v)) {
				isInt = false
			}
			// Any number is not a bool
			isBool = false
		case string:
			isInt = false
			isFloat = false
			isBool = false
		case bool:
			isInt = false
			isFloat = false
			// bool stays
		case map[string]any, []any:
			isInt = false
			isFloat = false
			isBool = false // string fallback
		default:
			// unknown -> string
			isInt = false
			isFloat = false
			isBool = false
		}
	}
	typeStr := "String"
	if isBool {
		typeStr = "Bool"
	} else if isInt {
		typeStr = "Int64"
	} else if isFloat {
		typeStr = "Float64"
	}
	if nullable {
		return fmt.Sprintf("Nullable(%s)", typeStr), true
	}
	return typeStr, false
}
