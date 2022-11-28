package repo

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepo struct {
	name   string
	reader *pgxpool.Pool
	writer *pgxpool.Pool
	table  string
	prefix string
	cols   []string
}

type QueryResults struct {
	poolCon *pgxpool.Conn
	rows    pgx.Rows
	row     pgx.Row
}

type ColumnsLang string

const (
	ColumnLangRu ColumnsLang = "russian"
	ColumnLangEn ColumnsLang = "english"
)

func NewPostgresRepo(reader, writer *pgxpool.Pool, name, table, prefix string, columns []string) *PostgresRepo {
	return &PostgresRepo{
		name:   name,
		reader: reader,
		writer: writer,
		table:  table,
		prefix: prefix,
		cols:   columns,
	}
}

func (r *PostgresRepo) SetRepoName(name string) {
	r.name = name
}

func (r PostgresRepo) GetRepoName() string {
	return r.name
}

func (r *PostgresRepo) SetReader(reader *pgxpool.Pool) {
	r.reader = reader
}

func (r PostgresRepo) GetReader() *pgxpool.Pool {
	return r.reader
}

func (r *PostgresRepo) SetWriter(writer *pgxpool.Pool) {
	r.writer = writer
}

func (r PostgresRepo) GetWriter() *pgxpool.Pool {
	return r.writer
}

func (r *PostgresRepo) SetTable(table string) {
	r.table = table
}

func (r *PostgresRepo) GetTable() string {
	return r.table
}

func (r *PostgresRepo) SetPrefix(prefix string) {
	r.prefix = prefix
}

func (r PostgresRepo) GetPrefix() string {
	return r.prefix
}

func (r *PostgresRepo) SetCols(cols []string) {
	r.cols = cols
}

func (r *PostgresRepo) GetCols() []string {
	return r.cols
}

func (r PostgresRepo) Column(name string, args ...string) string {
	prefix := r.prefix

	if len(args) >= 1 {
		prefix = args[0]
	}

	if prefix != "" {
		return fmt.Sprintf("%s.%s", prefix, name)
	}

	return name
}

func (r PostgresRepo) ColumnWithoutPrefix(name string) string {
	str := strings.Split(name, ".")
	if len(str) <= 1 {
		return name
	}

	return str[1]
}

func (r PostgresRepo) Columns(specificColumns ...string) []string {
	cols := r.cols
	if len(specificColumns) > 0 {
		cols = specificColumns
	}

	cc := make([]string, 0, len(cols))

	for _, c := range r.cols {
		cc = append(cc, fmt.Sprintf("%s.%s", r.prefix, c))
	}

	return cc
}

func (r PostgresRepo) ColumnsWithoutPrefix(specificColumns ...string) []string {
	cols := make([]string, 0, len(r.Columns(specificColumns...)))

	if len(specificColumns) == 0 {
		specificColumns = r.Columns()
	}

	for _, col := range specificColumns {
		str := strings.Split(col, ".")
		if len(str) <= 1 {
			cols = append(cols, col)

			continue
		}

		cols = append(cols, str[1])
	}

	return cols
}

func (r PostgresRepo) ColumnExists(column string) bool {
	columnSlice := strings.Split(column, ".")

	if len(columnSlice) == 2 { //nolint:gomnd // структура наименования с определением названия таблицы -- 2 элемента
		column = columnSlice[1]
	}

	for _, col := range r.ColumnsWithoutPrefix() {
		if col == column {
			return true
		}
	}

	return false
}

func (r PostgresRepo) TableName(args ...string) string {
	prefix := r.prefix

	if len(args) >= 1 {
		prefix = args[0]
	}

	return fmt.Sprintf("%s %s", r.table, prefix)
}

func (r PostgresRepo) TableNameWithoutPrefix() string {
	return r.table
}

func (r PostgresRepo) JoinOn(prefix string, on string) string {
	if prefix == "" {
		prefix = r.prefix
	}

	return r.TableName(prefix) + " on " + on
}

func (r PostgresRepo) Join(foreignColumns, thisTableColumn string, additionalWheres ...squirrel.Sqlizer) (string, []any) {
	stmt := fmt.Sprintf("%s on %s = %s", r.TableName(), thisTableColumn, foreignColumns)

	if len(additionalWheres) == 0 {
		return stmt, nil
	}

	args := make([]any, 0)

	for _, where := range additionalWheres {
		if where == nil {
			continue
		}

		q, moreArgs, _ := where.ToSql() //nolint:errcheck // полагаем, что ошибок в формировании подзапроса не будет

		stmt += fmt.Sprintf(" AND %s", q)

		if len(moreArgs) > 0 {
			args = append(args, moreArgs...)
		}
	}

	return stmt, args
}

func (r PostgresRepo) ReadRows(ctx context.Context, _sql string, args []any) (*QueryResults, error) {
	db, err := r.GetReader().Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s.readRows Acquire error: %w", r.GetRepoName(), err)
	}

	rows, err := db.Query(ctx, _sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s.readRows Query error: %w", r.GetRepoName(), err)
	}

	return &QueryResults{poolCon: db, rows: rows}, nil
}

func (r PostgresRepo) ReadRow(ctx context.Context, _sql string, args []any) (*QueryResults, error) {
	db, err := r.GetReader().Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s.ReadRow Acquire error: %w", r.GetRepoName(), err)
	}

	return &QueryResults{poolCon: db, row: db.QueryRow(ctx, _sql, args...)}, nil
}

func (r PostgresRepo) WriteData(ctx context.Context, _sql string, args []any) error {
	db, err := r.GetWriter().Acquire(ctx)
	if err != nil {
		return fmt.Errorf("%s.writeData Acquire error: %w", r.GetRepoName(), err)
	}

	defer db.Release()

	if _, err := db.Exec(ctx, _sql, args...); err != nil {
		return fmt.Errorf("%s.writeData query error: %w", r.GetRepoName(), err)
	}

	return nil
}

func (r PostgresRepo) WriteDataAndReadRow(ctx context.Context, _sql string, args []any) (*QueryResults, error) {
	db, err := r.GetWriter().Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s.WriteDataAndReadRow Acquire error: %w", r.GetRepoName(), err)
	}

	return &QueryResults{poolCon: db, row: db.QueryRow(ctx, _sql, args...)}, nil
}

func (r PostgresRepo) WriteDataAndReadRows(ctx context.Context, _sql string, args []any) (*QueryResults, error) {
	db, err := r.GetWriter().Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s.WriteDataAndReadRows Acquire error: %w", r.GetRepoName(), err)
	}

	rows, err := db.Query(ctx, _sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s.WriteDataAndReadRows Query error: %w", r.GetRepoName(), err)
	}

	return &QueryResults{poolCon: db, rows: rows}, nil
}

func (r PostgresRepo) ReadRowsError(err error) error {
	return fmt.Errorf("%s row scan error: %w", r.GetRepoName(), err)
}

func (r PostgresRepo) ScanRowError(err error) error {
	return fmt.Errorf("%s row scan error: %w", r.GetRepoName(), err)
}

func (r PostgresRepo) ScanRowsError(err error) error {
	return fmt.Errorf("%s rows scan error: %w", r.GetRepoName(), err)
}

func (r PostgresRepo) ScanCountRow(row pgx.Row) (int, error) {
	var count int

	if err := row.Scan(&count); err != nil {
		return 0, err
	}

	return count, nil
}

func (r PostgresRepo) TsVectorFromColumn(lang ColumnsLang, columns ...string) squirrel.Sqlizer {
	if len(columns) == 0 {
		return nil
	}

	return squirrel.Expr(fmt.Sprintf(
		`to_tsvector('%s', lower(%s))`,
		lang,
		strings.Join(columns, " || ' ' || "),
	))
}

func (r PostgresRepo) TsVectorFromData(lang ColumnsLang, args ...string) squirrel.Sqlizer {
	if len(args) == 0 {
		return nil
	}

	return squirrel.Expr(fmt.Sprintf(
		`to_tsvector('%s', lower('%s'))`,
		lang,
		strings.Join(args, "' || ' ' || '"),
		// r.collate(lang),
	))
}

func (qr QueryResults) Close() {
	if qr.rows != nil {
		qr.rows.Close()
	}

	qr.poolCon.Release()
}

func (qr *QueryResults) GetRows() pgx.Rows {
	return qr.rows
}

func (qr *QueryResults) GetRow() pgx.Row {
	return qr.row
}
