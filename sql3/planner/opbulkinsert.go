// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

type bulkInsertMapColumn struct {
	name    string
	expr    types.PlanExpression
	colType parser.ExprDataType
}

// bulkInsertOptions contains options for bulk insert
type bulkInsertOptions struct {
	// name of the file we're going to read
	sourceData string
	// number of rows in a batch
	batchSize int
	// stop after this many rows
	rowsLimit int
	// format specifier (CSV is the only one right now)
	format string
	// whether the source has a header row
	hasHeaderRow bool
	// whether we allow missing values for NDJSON jsonpath expressions
	allowMissingValues bool
	// input specifier (FILE is the only one right now)
	input string

	// target columns
	targetColumns []*qualifiedRefPlanExpression

	// transformations
	transformExpressions []types.PlanExpression

	// map expressions
	mapExpressions []*bulkInsertMapColumn
}

// PlanOpBulkInsert plan operator to handle INSERT.
type PlanOpBulkInsert struct {
	planner   *ExecutionPlanner
	tableName string
	options   *bulkInsertOptions
	warnings  []string
}

func NewPlanOpBulkInsert(p *ExecutionPlanner, tableName string, options *bulkInsertOptions) *PlanOpBulkInsert {
	return &PlanOpBulkInsert{
		planner:   p,
		tableName: tableName,
		options:   options,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpBulkInsert) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["tableName"] = p.tableName

	options := make(map[string]interface{})
	options["sourceData"] = p.options.sourceData
	options["batchSize"] = p.options.batchSize
	options["rowsLimit"] = p.options.rowsLimit
	options["format"] = p.options.format
	options["input"] = p.options.input
	options["hasHeaderRow"] = p.options.hasHeaderRow
	options["allowMissingValues"] = p.options.allowMissingValues

	colMap := make([]interface{}, 0)
	for _, m := range p.options.targetColumns {
		colMap = append(colMap, m.Plan())
	}
	options["targetColumns"] = colMap

	mapList := make([]interface{}, 0)
	for _, m := range p.options.mapExpressions {
		mapItem := make(map[string]interface{})
		options["name"] = m.name
		options["type"] = m.colType.TypeDescription()
		options["expr"] = m.expr.Plan()
		mapList = append(mapList, mapItem)
	}
	options["mapExpressions"] = mapList

	if p.options.transformExpressions != nil && len(p.options.transformExpressions) > 0 {
		transformList := make([]interface{}, 0)
		for _, m := range p.options.transformExpressions {
			transformList = append(transformList, m.Plan())
		}
		options["transformExpressions"] = transformList
	}
	result["options"] = options
	return result
}

func (p *PlanOpBulkInsert) String() string {
	return ""
}

func (p *PlanOpBulkInsert) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpBulkInsert) Warnings() []string {
	return p.warnings
}

func (p *PlanOpBulkInsert) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpBulkInsert) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpBulkInsert) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	switch strings.ToUpper(p.options.format) {
	case "CSV":
		return &bulkInsertLineRowIter{
			planner:   p.planner,
			tableName: p.tableName,
			options:   p.options,
			sourceIter: &bulkInsertSourceCSVRowIter{
				planner: p.planner,
				options: p.options,
			},
		}, nil

	case "NDJSON":
		return &bulkInsertNDJsonRowIter{
			planner:   p.planner,
			tableName: p.tableName,
			options:   p.options,
			sourceIter: &bulkInsertSourceNDJsonRowIter{
				planner: p.planner,
				options: p.options,
			},
		}, nil
	case "PARQUET":
		return &bulkInsertLineRowIter{
			planner:   p.planner,
			tableName: p.tableName,
			options:   p.options,
			sourceIter: &bulkInsertSourceParquetRowIter{
				planner: p.planner,
				options: p.options,
			},
		}, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected format '%s'", p.options.format)
	}
}

func (p *PlanOpBulkInsert) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpBulkInsert(p.planner, p.tableName, p.options), nil
}

type bulkInsertSourceCSVRowIter struct {
	planner   *ExecutionPlanner
	options   *bulkInsertOptions
	csvReader *csv.Reader

	closeFunc func()

	mapValues []int64

	hasStarted *struct{}
}

var _ types.RowIterator = (*bulkInsertSourceCSVRowIter)(nil)

func (i *bulkInsertSourceCSVRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {

		i.hasStarted = &struct{}{}

		// pre-calculate map values since these represent column offsets and will be constant for csv
		i.mapValues = []int64{}
		for _, mc := range i.options.mapExpressions {
			// this is csv so map value will be an int
			rawMapValue, err := mc.expr.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			mapValue, ok := rawMapValue.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type for mapValue '%T'", rawMapValue)
			}
			i.mapValues = append(i.mapValues, mapValue)
		}

		switch strings.ToUpper(i.options.input) {
		case "FILE":
			f, err := os.Open(i.options.sourceData)
			if err != nil {
				return nil, err
			}
			i.closeFunc = func() {
				f.Close()
			}

			i.csvReader = csv.NewReader(f)

		case "URL":
			response, err := http.Get(i.options.sourceData)
			if err != nil {
				return nil, err
			}
			i.closeFunc = func() {
				response.Body.Close()
			}
			if response.StatusCode != 200 {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("unexpected response %d", response.StatusCode))
			}
			i.csvReader = csv.NewReader(response.Body)

		case "STREAM":
			i.csvReader = csv.NewReader(strings.NewReader(i.options.sourceData))

		default:
			return nil, sql3.NewErrInternalf("unexpected input specification type '%s'", i.options.input)
		}

		i.csvReader.LazyQuotes = true
		i.csvReader.TrimLeadingSpace = true
		// skip header row if necessary
		if i.options.hasHeaderRow {
			_, err := i.csvReader.Read()
			if err == io.EOF {
				return nil, types.ErrNoMoreRows
			} else if err != nil {
				return nil, err
			}
		}
	}

	rec, err := i.csvReader.Read()
	if err == io.EOF {
		return nil, types.ErrNoMoreRows
	} else if err != nil {
		pe, ok := err.(*csv.ParseError)
		if ok {
			return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("csv parse error on line %d: %s", pe.Line, pe.Error()))
		}
		return nil, err
	}

	// now we do the mapping to the output row
	result := make([]interface{}, len(i.options.mapExpressions))
	for idx := range i.options.mapExpressions {
		mapExpressionResult := i.mapValues[idx]
		if !(mapExpressionResult >= 0 && int(mapExpressionResult) < len(rec)) {
			return nil, sql3.NewErrMappingFromDatasource(0, 0, i.options.sourceData, fmt.Sprintf("map index %d out of range", mapExpressionResult))
		}
		evalValue := rec[mapExpressionResult]

		mapColumn := i.options.mapExpressions[idx]
		switch mapColumn.colType.(type) {
		case *parser.DataTypeID, *parser.DataTypeInt:
			intVal, err := strconv.ParseInt(evalValue, 10, 64)
			if err != nil {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
			result[idx] = intVal

		case *parser.DataTypeIDSet:
			intVal, err := strconv.ParseInt(evalValue, 10, 64)
			if err != nil {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
			result[idx] = []int64{intVal}

		case *parser.DataTypeStringSet:
			result[idx] = []string{evalValue}

		case *parser.DataTypeTimestamp:
			intVal, err := strconv.ParseInt(evalValue, 10, 64)
			if err != nil {
				if tm, err := time.ParseInLocation(time.RFC3339Nano, evalValue, time.UTC); err == nil {
					result[idx] = tm
				} else if tm, err := time.ParseInLocation(time.RFC3339, evalValue, time.UTC); err == nil {
					result[idx] = tm
				} else if tm, err := time.ParseInLocation("2006-01-02", evalValue, time.UTC); err == nil {
					result[idx] = tm
				} else {
					return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
				}
			} else {
				result[idx] = time.UnixMilli(intVal).UTC()
			}

		case *parser.DataTypeString:
			result[idx] = evalValue

		case *parser.DataTypeBool:
			bval, err := strconv.ParseBool(evalValue)
			if err != nil {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
			result[idx] = bval

		case *parser.DataTypeDecimal:
			dval, err := pql.ParseDecimal(evalValue)
			if err != nil {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
			result[idx] = dval

		default:
			return nil, sql3.NewErrInternalf("unhandled type '%T'", mapColumn.colType)
		}
	}
	return result, nil
}

func (i *bulkInsertSourceCSVRowIter) Close(ctx context.Context) {
	if i.closeFunc != nil {
		i.closeFunc()
	}
}

type bulkInsertBasicRowIter interface {
	Next(context.Context) (types.Row, error)
	Close(context.Context)
}

type bulkInsertLineRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	options   *bulkInsertOptions
	linesRead int

	currentBatch [][]interface{}

	sourceIter bulkInsertBasicRowIter
}

var _ types.RowIterator = (*bulkInsertLineRowIter)(nil)

func (i *bulkInsertLineRowIter) Next(ctx context.Context) (types.Row, error) {
	defer i.sourceIter.Close(ctx)
	for {
		row, err := i.sourceIter.Next(ctx)
		if err != nil && err != types.ErrNoMoreRows {
			return nil, err
		}
		if err == types.ErrNoMoreRows {
			break
		}
		i.linesRead++

		if i.currentBatch == nil {
			i.currentBatch = make([][]interface{}, 0)
		}
		i.currentBatch = append(i.currentBatch, row)
		if len(i.currentBatch) >= i.options.batchSize {
			err := processBatch(ctx, i.planner, i.tableName, i.currentBatch, i.options)
			if err != nil {
				return nil, err
			}
			i.currentBatch = nil
			// update the counter for bulk insert batches
			pilosa.PerfCounterSQLBulkInsertBatchesSec.Add(1)
		}
		if i.options.rowsLimit > 0 && i.linesRead >= i.options.rowsLimit {
			break
		}
	}
	if len(i.currentBatch) > 0 {
		err := processBatch(ctx, i.planner, i.tableName, i.currentBatch, i.options)
		if err != nil {
			return nil, err
		}
		i.currentBatch = nil
		// update the counter for bulk insert batches
		pilosa.PerfCounterSQLBulkInsertBatchesSec.Add(1)
	}
	return nil, types.ErrNoMoreRows
}

type bulkInsertSourceNDJsonRowIter struct {
	planner *ExecutionPlanner
	options *bulkInsertOptions
	reader  *bufio.Scanner

	closeFunc func()

	mapExpressionResults []string
	pathExpressions      []gval.Evaluable

	hasStarted *struct{}
}

var _ types.RowIterator = (*bulkInsertSourceNDJsonRowIter)(nil)

func (i *bulkInsertSourceNDJsonRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {

		i.hasStarted = &struct{}{}

		builder := gval.Full(jsonpath.PlaceholderExtension())

		// pre-calculate map values since these represent ndjson expressions and will be constant
		i.mapExpressionResults = []string{}
		i.pathExpressions = []gval.Evaluable{}
		for _, mc := range i.options.mapExpressions {
			rawMapValue, err := mc.expr.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			mapValue, ok := rawMapValue.(string)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type for mapValue '%T'", rawMapValue)
			}
			i.mapExpressionResults = append(i.mapExpressionResults, mapValue)

			path, err := builder.NewEvaluable(mapValue)
			if err != nil {
				return nil, err
			}
			i.pathExpressions = append(i.pathExpressions, path)
		}

		switch strings.ToUpper(i.options.input) {
		case "FILE":
			f, err := os.Open(i.options.sourceData)
			if err != nil {
				return nil, err
			}

			i.closeFunc = func() {
				f.Close()
			}

			i.reader = bufio.NewScanner(f)

		case "URL":
			response, err := http.Get(i.options.sourceData)
			if err != nil {
				return nil, err
			}
			i.closeFunc = func() {
				response.Body.Close()
			}
			if response.StatusCode != 200 {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("unexpected response %d", response.StatusCode))
			}
			i.reader = bufio.NewScanner(response.Body)

		case "STREAM":
			i.reader = bufio.NewScanner(strings.NewReader(i.options.sourceData))

		default:
			return nil, sql3.NewErrInternalf("unexpected input specification type '%s'", i.options.input)
		}
	}

	for {
		if i.reader.Scan() {
			if err := i.reader.Err(); err != nil {
				return nil, err
			}

			jsonValue := i.reader.Text()
			jsonValue = strings.TrimSpace(jsonValue)
			if len(jsonValue) == 0 {
				continue
			}

			// now we do the mapping to the output row
			result := make([]interface{}, len(i.options.mapExpressions))

			// parse the json
			v := interface{}(nil)
			err := json.Unmarshal([]byte(jsonValue), &v)
			if err != nil {
				return nil, sql3.NewErrParsingJSON(0, 0, jsonValue, err.Error())
			}

			// type check against the output type of the map operation

			for idx, expr := range i.pathExpressions {

				evalValue, err := expr(ctx, v)
				if err != nil {
					if i.options.allowMissingValues && (strings.HasPrefix(err.Error(), "unknown key") || strings.HasPrefix(err.Error(), "unknown parameter")) {
						evalValue = nil
					} else {
						return nil, sql3.NewErrEvaluatingJSONPathExpr(0, 0, i.mapExpressionResults[idx], jsonValue, err.Error())
					}
				}

				// if nil (null) then return nil
				if evalValue == nil {
					result[idx] = nil
					continue
				}

				mapColumn := i.options.mapExpressions[idx]
				switch mapColumn.colType.(type) {
				case *parser.DataTypeID, *parser.DataTypeInt:

					switch v := evalValue.(type) {
					case float64:
						// if v is a whole number then make it an int
						if v == float64(int64(v)) {
							result[idx] = int64(v)
						} else {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}

					case []interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case string:
						intVal, err := strconv.ParseInt(v, 10, 64)
						if err != nil {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}
						result[idx] = intVal

					case bool:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				case *parser.DataTypeIDSet:
					switch v := evalValue.(type) {
					case float64:
						// if v is a whole number then make it an int, and then turn that into an idset
						if v == float64(int64(v)) {
							result[idx] = []int64{int64(v)}
						} else {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}

					case []interface{}:
						setValue := make([]int64, 0)
						for _, i := range v {
							switch v := i.(type) {
							case float64:
								if v == float64(int64(v)) {
									setValue = append(setValue, int64(v))
								} else {
									return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
								}
							case string:
								intVal, err := strconv.ParseInt(v, 10, 64)
								if err != nil {
									return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
								}
								setValue = append(setValue, int64(intVal))

							default:
								return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
							}
						}
						result[idx] = setValue

					case string:
						intVal, err := strconv.ParseInt(v, 10, 64)
						if err != nil {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}
						result[idx] = []int64{int64(intVal)}

					case bool:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				case *parser.DataTypeStringSet:
					switch v := evalValue.(type) {
					case float64:
						if v == float64(int64(v)) {
							result[idx] = []string{fmt.Sprintf("%d", int64(v))}
						} else {
							result[idx] = []string{fmt.Sprintf("%f", v)}
						}

					case []interface{}:
						setValue := make([]string, 0)
						for _, i := range v {
							f, ok := i.(string)
							if !ok {
								return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
							}
							setValue = append(setValue, f)
						}
						result[idx] = setValue

					case string:
						result[idx] = []string{v}

					case bool:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				case *parser.DataTypeTimestamp:
					switch v := evalValue.(type) {
					case float64:
						// if v is a whole number then make it an int
						if v == float64(int64(v)) {
							result[idx] = time.UnixMilli(int64(v)).UTC()
						} else {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}

					case []interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case string:
						if tm, err := time.ParseInLocation(time.RFC3339Nano, v, time.UTC); err == nil {
							result[idx] = tm
						} else if tm, err := time.ParseInLocation(time.RFC3339, v, time.UTC); err == nil {
							result[idx] = tm
						} else if tm, err := time.ParseInLocation("2006-01-02", v, time.UTC); err == nil {
							result[idx] = tm
						} else {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}

					case bool:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				case *parser.DataTypeString:
					switch v := evalValue.(type) {
					case float64:
						// if a whole number make it an int
						if v == float64(int64(v)) {
							result[idx] = fmt.Sprintf("%d", int64(v))
						} else {
							result[idx] = fmt.Sprintf("%f", v)
						}

					case []interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case string:
						result[idx] = v

					case bool:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				case *parser.DataTypeBool:
					switch v := evalValue.(type) {
					case float64:
						// if a whole number make it an int, and convert to a bool
						if v == float64(int64(v)) {
							result[idx] = v > 0
						} else {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}

					case []interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case string:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case bool:
						result[idx] = v

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				case *parser.DataTypeDecimal:
					switch v := evalValue.(type) {
					case float64:
						result[idx] = pql.FromFloat64(v)

					case []interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case string:
						// try to parse from a string
						dv, err := pql.ParseDecimal(v)
						if err != nil {
							return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())
						}
						result[idx] = dv

					case bool:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					case interface{}:
						return nil, sql3.NewErrTypeConversionOnMap(0, 0, v, mapColumn.colType.TypeDescription())

					default:
						return nil, sql3.NewErrInternalf("unhandled type '%T'", evalValue)
					}

				default:
					return nil, sql3.NewErrInternalf("unhandled type '%T'", mapColumn.colType)
				}
			}
			return result, nil
		}
		return nil, types.ErrNoMoreRows
	}
}

func (i *bulkInsertSourceNDJsonRowIter) Close(ctx context.Context) {
	if i.closeFunc != nil {
		i.closeFunc()
	}
}

type bulkInsertNDJsonRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	options   *bulkInsertOptions
	linesRead int

	currentBatch [][]interface{}

	sourceIter *bulkInsertSourceNDJsonRowIter
}

var _ types.RowIterator = (*bulkInsertNDJsonRowIter)(nil)

func (i *bulkInsertNDJsonRowIter) Next(ctx context.Context) (types.Row, error) {
	defer i.sourceIter.Close(ctx)
	for {
		row, err := i.sourceIter.Next(ctx)
		if err != nil && err != types.ErrNoMoreRows {
			return nil, err
		}
		if err == types.ErrNoMoreRows {
			break
		}
		i.linesRead++

		if i.currentBatch == nil {
			i.currentBatch = make([][]interface{}, 0)
		}
		i.currentBatch = append(i.currentBatch, row)
		if len(i.currentBatch) >= i.options.batchSize {
			err := processBatch(ctx, i.planner, i.tableName, i.currentBatch, i.options)
			if err != nil {
				return nil, err
			}
			i.currentBatch = nil
			// update the counter for bulk insert batches
			pilosa.PerfCounterSQLBulkInsertBatchesSec.Add(1)
		}
		if i.options.rowsLimit > 0 && i.linesRead >= i.options.rowsLimit {
			break
		}
	}
	if len(i.currentBatch) > 0 {
		err := processBatch(ctx, i.planner, i.tableName, i.currentBatch, i.options)
		if err != nil {
			return nil, err
		}
		i.currentBatch = nil
		// update the counter for bulk insert batches
		pilosa.PerfCounterSQLBulkInsertBatchesSec.Add(1)
	}
	return nil, types.ErrNoMoreRows
}

func processColumnValue(rawValue interface{}, targetType parser.ExprDataType) (types.PlanExpression, error) {
	if rawValue == nil {
		return newNullLiteralPlanExpression(), nil
	}

	switch targetType.(type) {
	case *parser.DataTypeID, *parser.DataTypeInt:
		ival, ok := rawValue.(int64)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected value type '%T'", rawValue)
		}

		return newIntLiteralPlanExpression(ival), nil

	case *parser.DataTypeIDSet:
		val, ok := rawValue.([]int64)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert '%s", rawValue)
		}
		members := make([]types.PlanExpression, 0)
		for _, m := range val {
			members = append(members, newIntLiteralPlanExpression(m))
		}
		return newExprSetLiteralPlanExpression(members, parser.NewDataTypeIDSet()), nil

	case *parser.DataTypeStringSet:
		val, ok := rawValue.([]string)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert '%s", rawValue)
		}
		members := make([]types.PlanExpression, 0)
		for _, m := range val {
			members = append(members, newStringLiteralPlanExpression(m))
		}
		return newExprSetLiteralPlanExpression(members, parser.NewDataTypeStringSet()), nil

	case *parser.DataTypeTimestamp:
		tval, ok := rawValue.(time.Time)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert '%s", rawValue)
		}
		return newDateLiteralPlanExpression(tval), nil

	case *parser.DataTypeString:
		sval, ok := rawValue.(string)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert '%s", rawValue)
		}
		return newStringLiteralPlanExpression(sval), nil

	case *parser.DataTypeBool:
		bval, ok := rawValue.(bool)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert '%s", rawValue)
		}
		return newBoolLiteralPlanExpression(bval), nil

	case *parser.DataTypeDecimal:
		dval, ok := rawValue.(pql.Decimal)
		if !ok {
			return nil, sql3.NewErrInternalf("unable to convert '%s", rawValue)
		}
		return newFloatLiteralPlanExpression(fmt.Sprintf("%f", dval.Float64())), nil

	default:
		return nil, sql3.NewErrInternalf("unhandled type '%T'", targetType)
	}
}

func processBatch(ctx context.Context, planner *ExecutionPlanner, tableName string, currentBatch [][]interface{}, options *bulkInsertOptions) error {
	insertValues := [][]types.PlanExpression{}

	// we're going to take a different path if transforms are specified
	// mostly for performmance reasons

	if len(options.transformExpressions) > 0 {
		// we have transformations so we are going to evaluate them and then build the insert tuple

		for _, row := range currentBatch {
			tupleValues := []types.PlanExpression{}

			// handle each transform
			for idx, mc := range options.transformExpressions {
				rawValue, err := mc.Evaluate(row)
				if err != nil {
					return err
				}

				// handle nulls
				if rawValue == nil {
					tupleValues = append(tupleValues, newNullLiteralPlanExpression())
					continue
				}

				tupleExpr, err := processColumnValue(rawValue, options.targetColumns[idx].dataType)
				if err != nil {
					return err
				}
				tupleValues = append(tupleValues, tupleExpr)
			}
			insertValues = append(insertValues, tupleValues)
		}
	} else {
		// we are just going to take the values from the source row and copy pasta them across
		// for each row in the batch add value to each mapped column

		for _, row := range currentBatch {

			tupleValues := []types.PlanExpression{}

			// handle each column
			for idx, rawValue := range row {
				tupleExpr, err := processColumnValue(rawValue, options.targetColumns[idx].dataType)
				if err != nil {
					return err
				}
				tupleValues = append(tupleValues, tupleExpr)
			}
			insertValues = append(insertValues, tupleValues)
		}
	}

	insert := &insertRowIter{
		planner:       planner,
		tableName:     tableName,
		targetColumns: options.targetColumns,
		insertValues:  insertValues,
	}

	_, err := insert.Next(ctx)
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}

	// update the counter for bulk inserts
	pilosa.PerfCounterSQLBulkInsertsSec.Add(int64(len(insertValues)))

	return nil
}

// /
// TODO(twg) 2023/01/23 need to refactor this
type colOrder struct {
	realColumn int
}
type parquetReader struct {
	table       *pilosa.BasicTable
	rowOffset   int
	columnOrder []colOrder
	row         []interface{}
}

func (pr *parquetReader) Read() ([]interface{}, error) {
	// need to read the row
	// and package it up according to the mappings
	if pr.rowOffset >= int(pr.table.NumRows()) {
		return nil, io.EOF // done
	}
	for i, col := range pr.columnOrder {
		// vprint.VV("check row:%v col:%v", pr.rowOffset, col.realColumn)
		pr.row[i] = pr.table.Get(col.realColumn, pr.rowOffset)
	}
	pr.rowOffset++
	return pr.row, nil
}

func process(typeMappings []*bulkInsertMapColumn, schema *arrow.Schema) ([]colOrder, error) {
	ret := make([]colOrder, len(typeMappings))
	find := func(n string) int {
		for i, x := range schema.Fields() {
			if n == x.Name {
				return i
			}
		}
		return -1
	}
	for i, column := range typeMappings {
		iname, err := column.expr.Evaluate(nil)
		if err != nil {
			return nil, err
		}
		columnIdx := find(iname.(string))
		if columnIdx < 0 {
			return nil, sql3.NewErrInternalf("unexpected type for mapping '%v' not found in parquet", iname)
		}
		ret[i] = colOrder{columnIdx}
	}
	return ret, nil
}

func NewParquetReader(ctx context.Context, mappings []*bulkInsertMapColumn, r *os.File, mem memory.Allocator) (*parquetReader, error) {
	pf, err := file.NewParquetReader(r)
	if err != nil {
		return nil, err
	}
	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, err
	}
	table, err := reader.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	m := &parquetReader{}
	m.table = pilosa.BasicTableFromArrow(table, mem)
	m.columnOrder, err = process(mappings, table.Schema())
	if err != nil {
		return nil, err
	}
	m.row = make([]interface{}, len(m.columnOrder))
	return m, nil
}

type bulkInsertSourceParquetRowIter struct {
	planner       *ExecutionPlanner
	options       *bulkInsertOptions
	parquetReader *parquetReader

	closeFunc func()

	hasStarted *struct{}
	pool       memory.Allocator
}

var _ types.RowIterator = (*bulkInsertSourceCSVRowIter)(nil)

func (i *bulkInsertSourceParquetRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {

		i.hasStarted = &struct{}{}
		i.pool = memory.NewGoAllocator()

		switch strings.ToUpper(i.options.input) {
		case "FILE":
			f, err := os.Open(i.options.sourceData)
			if err != nil {
				return nil, err
			}
			i.closeFunc = func() {
				f.Close()
			}
			i.parquetReader, err = NewParquetReader(ctx, i.options.mapExpressions, f, i.pool)
			if err != nil {
				return nil, sql3.NewErrInternalf("problems with parquet file '%v' '%v'", i.options.sourceData, err)
			}

		case "URL":
			response, err := http.Get(i.options.sourceData)
			if err != nil {
				return nil, err
			}
			if response.StatusCode != 200 {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("unexpected response %d", response.StatusCode))
			}
			defer response.Body.Close()
			// download to temp file first
			tmpFile, err := os.CreateTemp("", "BulkParquetFile.parquet")
			if err != nil {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("error creating tempfile %v", err))
			}
			i.closeFunc = func() {
				tmpFile.Close()
			}
			_, err = io.Copy(tmpFile, response.Body)
			if err != nil {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("error downloading url %v %v", i.options.sourceData, err))
			}
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.Seek(0, io.SeekStart)
			if err != nil {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("error reseting file for reading %v ", err))
			}

			i.parquetReader, err = NewParquetReader(ctx, i.options.mapExpressions, tmpFile, i.pool)
			if err != nil {
				return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("reading parquet file %v ", err))
			}

		default:
			return nil, sql3.NewErrInternalf("unexpected input specification type '%s'", i.options.input)
		}

	}

	rec, err := i.parquetReader.Read()
	if err == io.EOF {
		return nil, types.ErrNoMoreRows
	} else if err != nil {
		pe, ok := err.(*csv.ParseError)
		if ok {
			return nil, sql3.NewErrReadingDatasource(0, 0, i.options.sourceData, fmt.Sprintf("csv parse error on line %d: %s", pe.Line, pe.Error()))
		}
		return nil, err
	}

	// now we do the mapping to the output row
	// current assumption is float--> DECIMAL(n)
	result := make([]interface{}, len(i.options.mapExpressions))
	for idx := range i.options.mapExpressions {
		evalValue := rec[idx]
		mapColumn := i.options.mapExpressions[idx]
		switch mapColumn.colType.(type) {
		case *parser.DataTypeID, *parser.DataTypeInt:
			if intVal, ok := evalValue.(int64); ok {
				result[idx] = intVal
			} else {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
		case *parser.DataTypeIDSet:
			if intVal, ok := evalValue.(int64); ok {
				result[idx] = []int64{intVal}
			} else {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}

		case *parser.DataTypeStringSet:
			if stringVal, ok := evalValue.(string); ok {
				result[idx] = []string{stringVal}
			} else {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}

		case *parser.DataTypeTimestamp:
			if intVal, ok := evalValue.(int64); ok {
				result[idx] = time.UnixMilli(intVal).UTC()
			} else if stringVal, ok := evalValue.(string); ok {
				if tm, err := time.ParseInLocation(time.RFC3339Nano, stringVal, time.UTC); err == nil {
					result[idx] = tm
				} else if tm, err := time.ParseInLocation(time.RFC3339, stringVal, time.UTC); err == nil {
					result[idx] = tm
				} else if tm, err := time.ParseInLocation("2006-01-02", stringVal, time.UTC); err == nil {
					result[idx] = tm
				} else {
					return nil, sql3.NewErrTypeConversionOnMap(0, 0, stringVal, mapColumn.colType.TypeDescription())
				}
			}

		case *parser.DataTypeString:
			if stringVal, ok := evalValue.(string); ok {
				result[idx] = stringVal
			} else {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}

		case *parser.DataTypeBool:
			if boolVal, ok := evalValue.(bool); ok {
				result[idx] = boolVal
			} else {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
		case *parser.DataTypeDecimal:
			if floatVal, ok := evalValue.(float64); ok {
				result[idx] = pql.FromFloat64(floatVal)
			} else {
				return nil, sql3.NewErrTypeConversionOnMap(0, 0, evalValue, mapColumn.colType.TypeDescription())
			}
		default:
			return nil, sql3.NewErrInternalf("unhandled type '%T'", mapColumn.colType)
		}
	}
	return result, nil
}

func (i *bulkInsertSourceParquetRowIter) Close(ctx context.Context) {
	if i.closeFunc != nil {
		i.closeFunc()
	}
}
