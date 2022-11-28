package repo

import (
	"context"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type RepositoryInterface interface {
	GetReader() *pgxpool.Pool
	GetWriter() *pgxpool.Pool
	GetRepoName() string

	GetPrefix() string
	TableName(args ...string) string
	TableNameWithoutPrefix() string
	Column(columnName string, prefix ...string) string
	ColumnWithoutPrefix(columnName string) string
	Columns(specificColumns ...string) []string
	ColumnsWithoutPrefix(specificCols ...string) []string
	ColumnExists(column string) bool

	Join(foreignColumns, thisTableColumn string, additionalWheres ...squirrel.Sqlizer) (string, []any)
	JoinOn(prefix string, on string) string

	ReadRow(ctx context.Context, _sql string, args []any) (*QueryResults, error)
	ReadRows(ctx context.Context, _sql string, args []any) (*QueryResults, error)
	WriteData(ctx context.Context, _sql string, args []any) error
	WriteDataAndReadRow(ctx context.Context, _sql string, args []any) (*QueryResults, error)
	WriteDataAndReadRows(ctx context.Context, _sql string, args []any) (*QueryResults, error)

	ReadRowsError(err error) error
	ScanRowError(err error) error
	ScanRowsError(err error) error
	ScanCountRow(row pgx.Row) (int, error)

	TsVectorFromColumn(lang ColumnsLang, columns ...string) squirrel.Sqlizer
	TsVectorFromData(lang ColumnsLang, args ...string) squirrel.Sqlizer
}
