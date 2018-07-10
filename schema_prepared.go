package avro

import (
	"fmt"
	"reflect"
	"sync"
)

/*
Prepare optimizes a schema for decoding/encoding.

It makes a recursive copy of the schema given and returns an immutable
wrapper of the schema with some optimizations applied.
*/
func Prepare(schema Schema) Schema {
	return PrepareResolving(schema, schema)
}

// PrepareResolving handles reading where the writer and reader schemas
// differ but are compatible. Support is currently limited to adding or
// removing record fields.
func PrepareResolving(writer, reader Schema) Schema {
	job := prepareJob{
		seen: make(map[Schema]Schema),
	}
	return job.prepare(writer, reader)
}

type prepareJob struct {
	// the seen struct prevents infinite recursion by caching conversions.
	seen map[Schema]Schema
}

func (job *prepareJob) prepare(writer, reader Schema) Schema {
	output := writer
	switch writer := writer.(type) {
	case *RecordSchema:
		output = job.prepareRecordSchema(writer, reader.(*RecordSchema))
	case *RecursiveSchema:
		if seen := job.seen[writer.Actual]; seen != nil {
			return seen
		} else {
			return job.prepare(writer.Actual, writer.Actual)
		}
	case *UnionSchema:
		output = job.prepareUnionSchema(writer)
	case *ArraySchema:
		output = job.prepareArraySchema(writer)
	default:
		return writer
	}
	job.seen[writer] = output
	return output
}

func (job *prepareJob) prepareUnionSchema(input *UnionSchema) Schema {
	output := &UnionSchema{
		Types: make([]Schema, len(input.Types)),
	}
	for i, t := range input.Types {
		output.Types[i] = job.prepare(t, t)
	}
	return output
}

func (job *prepareJob) prepareArraySchema(input *ArraySchema) Schema {
	return &ArraySchema{
		Properties: input.Properties,
		Items:      job.prepare(input.Items, input.Items),
	}
}
func (job *prepareJob) prepareMapSchema(input *MapSchema) Schema {
	return &MapSchema{
		Properties: input.Properties,
		Values:     job.prepare(input.Values, input.Values),
	}
}

func (job *prepareJob) prepareRecordSchema(writer, reader *RecordSchema) *preparedRecordSchema {
	output := &preparedRecordSchema{
		RecordSchema: *writer,
		ReaderSchema: reader,
		pool:         sync.Pool{New: func() interface{} { return make(map[reflect.Type]*recordPlan) }},
	}
	output.Fields = nil
	for _, field := range writer.Fields {
		output.Fields = append(output.Fields, &SchemaField{
			Name:    field.Name,
			Doc:     field.Doc,
			Default: field.Default,
			Type:    job.prepare(field.Type, field.Type),
		})
	}
	return output
}

type preparedRecordSchema struct {
	RecordSchema // WriterSchema
	ReaderSchema *RecordSchema
	pool         sync.Pool
}

func (rs *preparedRecordSchema) getPlan(t reflect.Type) (plan *recordPlan, err error) {
	cache := rs.pool.Get().(map[reflect.Type]*recordPlan)
	if plan = cache[t]; plan != nil {
		rs.pool.Put(cache)
		return
	}

	// Use the reflectmap to get field info.
	ri := reflectEnsureRi(t)

	readerFieldNames := make(map[string]struct{})
	for _, schemaField := range rs.ReaderSchema.Fields {
		readerFieldNames[schemaField.Name] = struct{}{}
	}

	decodePlan := make([]structFieldPlan, len(rs.Fields))
	for i, schemafield := range rs.Fields {
		_, readerHasField := readerFieldNames[schemafield.Name]
		index, ok := ri.names[schemafield.Name]
		if !ok && readerHasField {
			err = fmt.Errorf("Type %v does not have field %s required for decoding schema", t, schemafield.Name)
		}
		entry := &decodePlan[i]
		entry.schema = schemafield.Type
		entry.name = schemafield.Name
		entry.index = index
		entry.dec = specificDecoder(entry)
	}

	plan = &recordPlan{
		// Over time, we will create decode/encode plans for more things.
		decodePlan: decodePlan,
	}
	cache[t] = plan
	rs.pool.Put(cache)
	return
}

// This is used
var sdr sDatumReader

type recordPlan struct {
	decodePlan []structFieldPlan
}

// For right now, until we implement more optimizations,
// we have a lot of cases we want a *RecordSchema. This makes it a bit easier to deal with.
func assertRecordSchema(s Schema) *RecordSchema {
	rs, ok := s.(*RecordSchema)
	if !ok {
		rs = &s.(*preparedRecordSchema).RecordSchema
	}
	return rs
}
