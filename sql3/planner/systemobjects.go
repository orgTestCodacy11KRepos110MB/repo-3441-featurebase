// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"encoding/json"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

type viewSystemObject struct {
	name      string
	statement string
}

type modelSystemObject struct {
	name         string
	status       string
	modelType    string
	labels       []string
	inputColumns []string
}

func (p *ExecutionPlanner) ensureViewsSystemTableExists() error {
	_, err := p.schemaAPI.TableByName(context.Background(), "fb_views")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}

		//  create table fb_views (
		// 		_id string
		//		name string
		//		statement string
		//		owner string
		//		updated_by string
		//		created_at timestamp
		//		updated_at timestamp
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_views",
			failIfExists:  false,
			isKeyed:       true,
			keyPartitions: 0,
			columns: []*createTableField{
				{
					planner:  p,
					name:     "name",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "statement",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "owner",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "updated_by",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "created_at",
					typeName: dax.BaseTypeTimestamp,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds),
					},
				},
				{
					planner:  p,
					name:     "updated_at",
					typeName: dax.BaseTypeTimestamp,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds),
					},
				},
			},
			description: "system table for views",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) getViewByName(name string) (*viewSystemObject, error) {
	err := p.ensureViewsSystemTableExists()
	if err != nil {
		return nil, err
	}

	tbl, err := p.schemaAPI.TableByName(context.Background(), "fb_views")
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, "fb_views")
	}

	cols := make([]string, len(tbl.Fields))
	for i, c := range tbl.Fields {
		cols[i] = string(c.Name)
	}

	iter := &tableScanRowIter{
		planner:   p,
		tableName: "fb_views",
		columns:   cols,
		predicate: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(name),
			parser.NewDataTypeBool(),
		),
		topExpr: nil,
	}

	row, err := iter.Next(context.Background())
	if err != nil {
		if err == types.ErrNoMoreRows {
			// view does not exist
			return nil, nil
		}
		return nil, err
	}

	return &viewSystemObject{
		name:      row[1].(string),
		statement: row[2].(string),
	}, nil
}

func (p *ExecutionPlanner) insertView(view *viewSystemObject) error {
	err := p.ensureViewsSystemTableExists()
	if err != nil {
		return err
	}

	createTime := time.Now().UTC()

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_views",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "name", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "statement", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "owner", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "created_at", 0, parser.NewDataTypeTimestamp()),
			newQualifiedRefPlanExpression("fb_views", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.statement),
				newStringLiteralPlanExpression(""),
				newStringLiteralPlanExpression(""),
				newDateLiteralPlanExpression(createTime),
				newDateLiteralPlanExpression(createTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) updateView(view *viewSystemObject) error {
	err := p.ensureViewsSystemTableExists()
	if err != nil {
		return err
	}

	updateTime := time.Now().UTC()

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_views",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "statement", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.statement),
				newStringLiteralPlanExpression(""),
				newDateLiteralPlanExpression(updateTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) deleteView(viewName string) error {
	err := p.ensureViewsSystemTableExists()
	if err != nil {
		return err
	}

	iter := &filteredDeleteRowIter{
		planner:   p,
		tableName: "fb_views",
		filter: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(viewName),
			parser.NewDataTypeBool(),
		),
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) ensureModelsSystemTableExists() error {
	_, err := p.schemaAPI.TableByName(context.Background(), "fb_models")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}
		//  create table fb_models (
		// 		_id string
		//		name string
		//		status string
		//		model_type string
		//		labels string --this is an array of string, we'll store it as a json object until we have string[] type in sql
		//		input_columns string  --this is an array for string, we'll store it as a json object until we have string[] type in sql
		//		owner string
		//		updated_by string
		//		created_at timestamp
		//		updated_at timestamp
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_models",
			failIfExists:  false,
			isKeyed:       true,
			keyPartitions: 0,
			columns: []*createTableField{
				{
					planner:  p,
					name:     "name",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "status",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "model_type",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "labels",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "input_columns",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "owner",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "updated_by",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "created_at",
					typeName: dax.BaseTypeTimestamp,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds),
					},
				},
				{
					planner:  p,
					name:     "updated_at",
					typeName: dax.BaseTypeTimestamp,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds),
					},
				},
			},
			description: "system table for models",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) ensureModelDataSystemTableExists() error {
	_, err := p.schemaAPI.TableByName(context.Background(), "fb_model_data")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}
		//  create table fb_model_data (
		// 		_id string
		//		model_id string
		//		data string
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_model_data",
			failIfExists:  false,
			isKeyed:       true,
			keyPartitions: 0,
			columns: []*createTableField{
				{
					planner:  p,
					name:     "model_id",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "data",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
			},
			description: "system table for model data",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) getModelByName(name string) (*modelSystemObject, error) {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return nil, err
	}

	tbl, err := p.schemaAPI.TableByName(context.Background(), "fb_models")
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, "fb_models")
	}

	cols := make([]string, len(tbl.Fields))
	for i, c := range tbl.Fields {
		cols[i] = string(c.Name)
	}

	iter := &tableScanRowIter{
		planner:   p,
		tableName: "fb_models",
		columns:   cols,
		predicate: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(name),
			parser.NewDataTypeBool(),
		),
		topExpr: nil,
	}

	row, err := iter.Next(context.Background())
	if err != nil {
		if err == types.ErrNoMoreRows {
			// model does not exist
			return nil, nil
		}
		return nil, err
	}

	labels := make([]string, 0)
	err = json.Unmarshal([]byte(row[4].(string)), &labels)
	if err != nil {
		return nil, err
	}

	inputColumns := make([]string, 0)
	err = json.Unmarshal([]byte(row[5].(string)), &inputColumns)
	if err != nil {
		return nil, err
	}

	return &modelSystemObject{
		name:         row[1].(string),
		status:       row[2].(string),
		modelType:    row[3].(string),
		labels:       labels,
		inputColumns: inputColumns,
	}, nil
}

func (p *ExecutionPlanner) insertModel(model *modelSystemObject) error {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return err
	}

	createTime := time.Now().UTC()

	labelJson, err := json.Marshal(model.labels)
	if err != nil {
		return err
	}

	inputColumnsJson, err := json.Marshal(model.inputColumns)
	if err != nil {
		return err
	}

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_models",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "name", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "status", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "model_type", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "labels", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "input_columns", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "owner", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "created_at", 0, parser.NewDataTypeTimestamp()),
			newQualifiedRefPlanExpression("fb_models", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(model.name),
				newStringLiteralPlanExpression(model.name),
				newStringLiteralPlanExpression(model.status),
				newStringLiteralPlanExpression(model.modelType),
				newStringLiteralPlanExpression(string(labelJson)),
				newStringLiteralPlanExpression(string(inputColumnsJson)),
				newStringLiteralPlanExpression(""),
				newStringLiteralPlanExpression(""),
				newDateLiteralPlanExpression(createTime),
				newDateLiteralPlanExpression(createTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) updateModel(model *modelSystemObject) error {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return err
	}

	updateTime := time.Now().UTC()

	labelJson, err := json.Marshal(model.labels)
	if err != nil {
		return err
	}

	inputColumnsJson, err := json.Marshal(model.inputColumns)
	if err != nil {
		return err
	}

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_models",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "status", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "model_type", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "labels", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "input_columns", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(model.name),
				newStringLiteralPlanExpression(model.status),
				newStringLiteralPlanExpression(model.modelType),
				newStringLiteralPlanExpression(string(labelJson)),
				newStringLiteralPlanExpression(string(inputColumnsJson)),
				newStringLiteralPlanExpression(""),
				newDateLiteralPlanExpression(updateTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}
