package log

import (
	"reflect"

	"github.com/openimsdk/tools/utils/datautil"
)

const summarizeLen = 30

type SummarizeOption func(*summarizeConfig)

func WithSummarizeLimit(limit int) SummarizeOption {
	return func(c *summarizeConfig) { c.limit = &limit }
}

func WithSummarizeName(name string) SummarizeOption {
	return func(c *summarizeConfig) { c.name = datautil.ToPtr(name) }
}

type summarizeConfig struct {
	name  *string
	limit *int
}

func newSummarizeConfig(opts ...SummarizeOption) *summarizeConfig {
	c := &summarizeConfig{
		limit: datautil.ToPtr(summarizeLen),
	}

	datautil.Foreach(opts, func(opt SummarizeOption) { opt(c) })

	if c.limit != nil && *c.limit < 0 {
		zero := 0
		c.limit = &zero
	}

	return c
}

func summarize(obj any, kind reflect.Kind, sample func(rv reflect.Value, limit int) any, opts ...SummarizeOption) any {
	config := newSummarizeConfig(opts...)

	rv := reflect.ValueOf(obj)
	if !rv.IsValid() || kind != rv.Kind() {
		return obj
	}

	total := rv.Len()
	sampleName := datautil.IfNil(config.name, "sample")
	limit := datautil.IfNil(config.limit, summarizeLen)

	sampleValue := obj

	if total > limit {
		sampleValue = sample(rv, limit)
	}

	return map[string]any{
		sampleName: sampleValue,
		"total":    total,
	}
}

func SummarizeMap(m any, opts ...SummarizeOption) any {
	return summarize(m, reflect.Map, func(rv reflect.Value, limit int) any {
		sample := reflect.MakeMapWithSize(rv.Type(), limit)

		iter := rv.MapRange()
		for count := 0; iter.Next() && count < limit; count++ {
			sample.SetMapIndex(iter.Key(), iter.Value())
		}

		return sample.Interface()
	}, opts...)
}

func SummarizeSlice(s any, opts ...SummarizeOption) any {
	return summarize(s, reflect.Slice, func(rv reflect.Value, limit int) any {
		return rv.Slice(0, limit).Interface()
	}, opts...)
}
