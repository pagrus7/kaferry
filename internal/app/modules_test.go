package app

import (
	"testing"

	"github.com/pagrus7/kaferry/internal/config"
	"github.com/pagrus7/kaferry/internal/plan"
	"github.com/pagrus7/kaferry/internal/sql"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestPlanningComposition(t *testing.T) {
	// Given
	var parser sql.Parser
	var planner plan.Builder
	var loader config.Reader
	app := fx.New(fx.NopLogger, PlanningModules, fx.Populate(&parser, &planner, &loader))
	require.NoError(t, app.Err())
	statement, err := parser.Parse("INSERT INTO ARCHIVE 'b' SELECT * FROM ARCHIVE 'a' WITH (GROUP='g') ")
	require.NoError(t, err)
	cfg, err := loader.Load("")
	require.NoError(t, err)

	// When
	_, buildErr := planner.Build(statement, cfg.Connections)

	// Then
	require.NoError(t, buildErr)
}
