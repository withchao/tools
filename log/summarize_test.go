package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSummarizeSlice(t *testing.T) {
	t.Run("small slice returns summary with original", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := SummarizeSlice(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["sample"])
		assert.Equal(t, 3, summary["total"])
	})

	t.Run("slice at default limit boundary returns original", func(t *testing.T) {
		input := make([]int, summarizeLen)
		for i := range input {
			input[i] = i
		}
		result := SummarizeSlice(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["sample"])
		assert.Equal(t, summarizeLen, summary["total"])
	})

	t.Run("large slice returns sampled first N elements", func(t *testing.T) {
		total := 100
		input := make([]int, total)
		for i := range input {
			input[i] = i
		}
		result := SummarizeSlice(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, total, summary["total"])

		sample, ok := summary["sample"].([]int)
		assert.True(t, ok)
		assert.Len(t, sample, summarizeLen)
		assert.Equal(t, input[:summarizeLen], sample)
	})

	t.Run("empty slice returns summary with empty sample", func(t *testing.T) {
		input := []string{}
		result := SummarizeSlice(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Empty(t, summary["sample"])
		assert.Equal(t, 0, summary["total"])
	})

	t.Run("nil typed slice returns summary with nil sample", func(t *testing.T) {
		var input []int = nil
		result := SummarizeSlice(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Nil(t, summary["sample"])
		assert.Equal(t, 0, summary["total"])
	})

	t.Run("untyped nil returns nil", func(t *testing.T) {
		result := SummarizeSlice(nil)
		assert.Nil(t, result)
	})

	t.Run("non-slice type returns original unchanged", func(t *testing.T) {
		input := "not a slice"
		result := SummarizeSlice(input)
		assert.Equal(t, input, result)
	})

	t.Run("custom limit truncates at specified value", func(t *testing.T) {
		input := make([]int, 50)
		for i := range input {
			input[i] = i
		}
		customLimit := 5
		result := SummarizeSlice(input, WithSummarizeLimit(customLimit))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, 50, summary["total"])

		sample, ok := summary["sample"].([]int)
		assert.True(t, ok)
		assert.Len(t, sample, customLimit)
		assert.Equal(t, input[:customLimit], sample)
	})

	t.Run("custom limit larger than total returns original", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := SummarizeSlice(input, WithSummarizeLimit(100))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["sample"])
		assert.Equal(t, 3, summary["total"])
	})

	t.Run("custom name changes sample key", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := SummarizeSlice(input, WithSummarizeName("items"))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["items"])
		assert.Equal(t, 3, summary["total"])
		assert.NotContains(t, summary, "sample")
	})

	t.Run("negative limit becomes zero and samples empty", func(t *testing.T) {
		input := []int{1, 2, 3}
		result := SummarizeSlice(input, WithSummarizeLimit(-5))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, 3, summary["total"])

		sample, ok := summary["sample"].([]int)
		assert.True(t, ok)
		assert.Empty(t, sample)
	})

	t.Run("both custom name and limit work together", func(t *testing.T) {
		input := make([]int, 100)
		for i := range input {
			input[i] = i
		}
		result := SummarizeSlice(input,
			WithSummarizeName("top"),
			WithSummarizeLimit(2),
		)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, 100, summary["total"])

		sample, ok := summary["top"].([]int)
		assert.True(t, ok)
		assert.Len(t, sample, 2)
		assert.Equal(t, []int{0, 1}, sample)
	})
}

func TestSummarizeMap(t *testing.T) {
	t.Run("small map returns summary with original", func(t *testing.T) {
		input := map[string]int{"a": 1, "b": 2, "c": 3}
		result := SummarizeMap(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["sample"])
		assert.Equal(t, 3, summary["total"])
	})

	t.Run("map at default limit boundary returns original", func(t *testing.T) {
		input := make(map[int]struct{}, summarizeLen)
		for i := 0; i < summarizeLen; i++ {
			input[i] = struct{}{}
		}
		result := SummarizeMap(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["sample"])
		assert.Equal(t, summarizeLen, summary["total"])
	})

	t.Run("large map returns sampled entries", func(t *testing.T) {
		total := 100
		input := make(map[int]int, total)
		for i := 0; i < total; i++ {
			input[i] = i * 2
		}
		result := SummarizeMap(input, WithSummarizeLimit(5))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, total, summary["total"])

		sample, ok := summary["sample"].(map[int]int)
		assert.True(t, ok)
		assert.Len(t, sample, 5)

		for k, v := range sample {
			assert.Equal(t, input[k], v)
		}
	})

	t.Run("empty map returns summary with nil sample", func(t *testing.T) {
		input := map[string]int{}
		result := SummarizeMap(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, 0, summary["total"])
		_, ok = summary["sample"].(map[string]int)
		assert.True(t, ok)
		assert.Empty(t, summary["sample"])
	})

	t.Run("nil typed map returns summary with nil sample", func(t *testing.T) {
		var input map[string]int = nil
		result := SummarizeMap(input)
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Nil(t, summary["sample"])
		assert.Equal(t, 0, summary["total"])
	})

	t.Run("untyped nil returns nil", func(t *testing.T) {
		result := SummarizeMap(nil)
		assert.Nil(t, result)
	})

	t.Run("non-map type returns original unchanged", func(t *testing.T) {
		input := 42
		result := SummarizeMap(input)
		assert.Equal(t, input, result)
	})

	t.Run("custom name changes sample key", func(t *testing.T) {
		input := map[string]int{"a": 1}
		result := SummarizeMap(input, WithSummarizeName("entry"))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, input, summary["entry"])
		assert.Equal(t, 1, summary["total"])
		assert.NotContains(t, summary, "sample")
	})

	t.Run("negative limit becomes zero for sampling", func(t *testing.T) {
		input := map[string]int{"a": 1, "b": 2, "c": 3}
		result := SummarizeMap(input, WithSummarizeLimit(-1))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, 3, summary["total"])

		sample, ok := summary["sample"].(map[string]int)
		assert.True(t, ok)
		assert.Empty(t, sample)
	})

	t.Run("zero limit samples empty map", func(t *testing.T) {
		input := map[string]int{"a": 1, "b": 2}
		result := SummarizeMap(input, WithSummarizeLimit(0))
		summary, ok := result.(map[string]any)
		assert.True(t, ok)
		assert.Equal(t, 2, summary["total"])

		sample, ok := summary["sample"].(map[string]int)
		assert.True(t, ok)
		assert.Empty(t, sample)
	})
}

func TestSummarizeWithLimit(t *testing.T) {
	t.Run("SummarizeWithLimit returns a function", func(t *testing.T) {
		opt := WithSummarizeLimit(10)
		assert.NotNil(t, opt)
	})

	t.Run("SummarizeWithLimit applied via config", func(t *testing.T) {
		c := newSummarizeConfig(WithSummarizeLimit(5))
		assert.Equal(t, 5, *c.limit)
	})
}

func TestSummarizeWithName(t *testing.T) {
	t.Run("SummarizeWithName returns a function", func(t *testing.T) {
		opt := WithSummarizeName("custom")
		assert.NotNil(t, opt)
	})

	t.Run("SummarizeWithName applied via config", func(t *testing.T) {
		c := newSummarizeConfig(WithSummarizeName("custom"))
		assert.Equal(t, "custom", *c.name)
	})
}

func TestNewSummarizeConfig(t *testing.T) {
	t.Run("default config uses summarizeLen", func(t *testing.T) {
		c := newSummarizeConfig()
		assert.Equal(t, summarizeLen, *c.limit)
		assert.Nil(t, c.name)
	})

	t.Run("negative limit clamped to zero", func(t *testing.T) {
		c := newSummarizeConfig(WithSummarizeLimit(-10))
		assert.Equal(t, 0, *c.limit)
	})

	t.Run("zero limit stays zero", func(t *testing.T) {
		c := newSummarizeConfig(WithSummarizeLimit(0))
		assert.Equal(t, 0, *c.limit)
	})

	t.Run("multiple options applied in order", func(t *testing.T) {
		c := newSummarizeConfig(
			WithSummarizeLimit(50),
			WithSummarizeName("data"),
		)
		assert.Equal(t, 50, *c.limit)
		assert.Equal(t, "data", *c.name)
	})
}
