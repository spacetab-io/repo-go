package repo

import (
	"context"
	"fmt"

	zapadapter "github.com/jackc/pgx-zap"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/spacetab-io/configuration-structs-go/v2/contracts"
	"go.uber.org/zap"
)

const (
	failureCode = 1
)

func PGConnect(cfg contracts.DatabaseCfgInterface, logger *zap.Logger, logLvl string) (*pgxpool.Pool, error) {
	pgxConfig, err := pgxpool.ParseConfig(cfg.GetDSN())
	if err != nil {
		return nil, err
	}

	pgxConfig.MaxConnLifetime, pgxConfig.MaxConns, pgxConfig.MinConns = cfg.GetConnectionParams()

	pgxConfig.ConnConfig.RuntimeParams = map[string]string{"standard_conforming_strings": "on"}
	pgxConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		// Так как БД esat находится на той же СУБД, что и de-api, мы получаем
		// наследование глобального datestyle и на бд esat. Чтобы поменять datestyle,
		// нужно поменять в конфиге postgres для всех БД, что может убить работу
		// de-api. Для того чтобы обойти это, мы выставим datestyle для коннекта,
		// который будет использовать только esat. И волки целы и овцы сыты.
		// Но конфигурацию БД нужно бы поменять...
		rows, err := conn.Query(ctx, "SET datestyle = 'ISO, DMY'")
		if err != nil {
			return fmt.Errorf("AfterConnect SET datestyle error: %w", err)
		}

		rows.Close()

		return nil
	}

	trLevel, err := tracelog.LogLevelFromString(logLvl)
	if err != nil {
		return nil, err
	}

	pgxConfig.ConnConfig.Tracer = &tracelog.TraceLog{
		Logger:   zapadapter.NewLogger(logger.WithOptions(zap.WithCaller(false))),
		LogLevel: trLevel,
	}

	db, err := pgxpool.NewWithConfig(context.Background(), pgxConfig)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(context.Background()); err != nil {
		return nil, err
	}

	return db, nil
}
