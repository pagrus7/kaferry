package app

import (
	"go.uber.org/fx"
	"kafka-fwd2/internal/config"
	"kafka-fwd2/internal/plan"
	"kafka-fwd2/internal/sql"
	"testing"
)

func TestPlanningComposition(t *testing.T) {
	var parser sql.Parser
	var planner plan.Builder
	var loader config.Reader
	app := fx.New(fx.NopLogger, PlanningModules, fx.Populate(&parser, &planner, &loader))
	if err := app.Err(); err != nil {
		t.Fatal(err)
	}
	statement, err := parser.Parse("INSERT INTO FILE cbor('b') SELECT * FROM FILE cbor('a') OPTIONS(GROUP='g')")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := loader.Load("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := planner.Build(statement, cfg.Connections); err != nil {
		t.Fatal(err)
	}
}
