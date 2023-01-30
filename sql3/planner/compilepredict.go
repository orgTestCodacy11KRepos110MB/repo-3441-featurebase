// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compilePredictStatement compiles a parser.PredictStatement AST into a PlanOperator
func (p *ExecutionPlanner) compilePredictStatement(stmt *parser.PredictStatement) (types.PlanOperator, error) {

	// go get the model
	modelName := parser.IdentName(stmt.ModelName)

	// does the model exist
	obj, err := p.getModelByName(modelName)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, sql3.NewErrInternalf("model '%s' not found", modelName)
	}

	selOp, err := p.compileSelectStatement(stmt.InputQuery, true)
	if err != nil {
		return nil, err
	}

	query := NewPlanOpQuery(p, NewPlanOpPredict(p, obj, selOp), p.sql)
	return query, nil
}

func (p *ExecutionPlanner) analyzePredictStatement(stmt *parser.PredictStatement) error {

	// analyze the select
	_, err := p.analyzeSelectStatement(stmt.InputQuery)
	if err != nil {
		return err
	}

	return nil
}
