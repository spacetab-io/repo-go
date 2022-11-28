package repo

import (
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/stretchr/testify/assert"
	"gitlab.worldskills.ru/worldskills/dpws/etc.git/v5/configuration"
)

var testLimits = &configuration.Limits{
	TitleLen: 4096,
	TextLen:  12800,
}

func TestRepositoryStuff_tsVector(t *testing.T) {
	type testCase struct {
		name string
		in   []string
		exp  squirrel.Sqlizer
	}

	tcs := []testCase{
		{
			name: "один аргумент",
			in:   []string{"question"},
			exp:  squirrel.Expr(`to_tsvector('russian', lower(question))`),
		},
		{
			name: "два аргумента",
			in:   []string{"question", "answer"},
			exp:  squirrel.Expr(`to_tsvector('russian', lower(question || ' ' || answer))`),
		},
		{
			name: "без аргументов",
			in:   nil,
			exp:  nil,
		},
	}

	t.Parallel()

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := RepositoryStuff{}

			assert.Equal(t, tc.exp, r.tsVectorFromColumn(ColumnLangRu, tc.in...))
		})
	}
}
