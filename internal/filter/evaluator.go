package filter

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
)

type Evaluator struct {
	enabled bool
	prg     cel.Program
}

// NewCEL builds a CEL evaluator for an expression working over a flattened map.
// Variables:
// - flat: map<string, dyn>
func NewCEL(expression string, enabled bool) (*Evaluator, error) {
	if !enabled || expression == "" {
		return &Evaluator{enabled: false}, nil
	}
	env, err := cel.NewEnv(
		cel.Declarations(
			decls.NewVar("flat", decls.NewMapType(decls.String, decls.Dyn)),
		),
		cel.HomogeneousAggregateLiterals(),
	)
	if err != nil {
		return nil, err
	}
	ast, iss := env.Compile(expression)
	if iss != nil && iss.Err() != nil {
		return nil, iss.Err()
	}
	prg, err := env.Program(ast)
	if err != nil {
		return nil, err
	}
	return &Evaluator{enabled: true, prg: prg}, nil
}

// Allow returns true if the message with given flattened fields should be kept.
func (e *Evaluator) Allow(flat map[string]any) bool {
	b, err := e.Evaluate(flat)
	if err != nil {
		return false
	}
	return b
}

// Evaluate returns the boolean result and any evaluation error.
func (e *Evaluator) Evaluate(flat map[string]any) (bool, error) {
	if e == nil || !e.enabled || e.prg == nil {
		return true, nil
	}
	if flat == nil {
		flat = map[string]any{}
	}
	out, _, err := e.prg.Eval(map[string]any{"flat": flat})
	if err != nil {
		return false, err
	}
	b, ok := out.Value().(bool)
	if !ok {
		return false, nil
	}
	return b, nil
}
