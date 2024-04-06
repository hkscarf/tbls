package clickhouse

import (
	"database/sql"

	"github.com/k1LoW/tbls/schema"
	"github.com/pkg/errors"
	"github.com/ClickHouse/clickhouse-go/v2"
)

type Clickhouse struct {
	db *sql.DB
}

func New(db *sql.DB) *Clickhouse {
	return &Clickhouse{
		db: db,
	}
}

func (sf *Clickhouse) Analyze(s *schema.Schema) error {
	d, err := sf.Info()
	if err != nil {
		return errors.WithStack(err)
	}
	s.Driver = d

	// FIXME TODO NOTE table_column not present in my local CH instance, but is present in docs - https://clickhouse.com/docs/en/operations/system-tables/information_schema#tables
	// tableRows, err := sf.db.Query(`SELECT table_name, table_type, table_comment FROM information_schema.tables WHERE table_schema = ? order by table_name`, s.Name)
	tableRows, err := sf.db.Query(`SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = ? order by table_name`, s.Name)
	if err != nil {
		return errors.WithStack(err)
	}
	defer tableRows.Close()

	tables := []*schema.Table{}

	for tableRows.Next() {
		var (
			tableName string
			tableType string
			comment   sql.NullString
		)
		if err := tableRows.Scan(&tableName, &tableType, &comment); err != nil {
			return errors.WithStack(err)
		}
		table := &schema.Table{
			Name:    tableName,
			Type:    tableType,
			Comment: comment.String,
		}

		// var getDDLObjectType string
		// if tableType == "BASE TABLE" {
		// 	getDDLObjectType = "table"
		// } else if tableType == "VIEW" {
		// 	getDDLObjectType = "view"
		// }
		// if getDDLObjectType != "" {
		// 	tableDefRows, err := sf.db.Query(`SELECT GET_DDL(?, ?)`, getDDLObjectType, tableName)
		// 	if err != nil {
		// 		return errors.WithStack(err)
		// 	}
		// 	defer tableDefRows.Close()
		// 	for tableDefRows.Next() {
		// 		var tableDef string
		// 		err := tableDefRows.Scan(&tableDef)
		// 		if err != nil {
		// 			return errors.WithStack(err)
		// 		}
		// 		table.Def = tableDef
		// 	}
		// }

		// columns, comments
		columnRows, err := sf.db.Query(`select column_name, column_default, is_nullable, data_type, column_comment
from information_schema.columns
where table_schema = ? and table_name = ? order by ordinal_position`, s.Name, tableName)
		if err != nil {
			return errors.WithStack(err)
		}
		defer columnRows.Close()
		columns := []*schema.Column{}
		for columnRows.Next() {
			var (
				columnName    string
				columnDefault sql.NullString
				isNullable    string
				dataType      string
				columnComment sql.NullString
			)
			err = columnRows.Scan(
				&columnName,
				&columnDefault,
				&isNullable,
				&dataType,
				&columnComment,
			)
			if err != nil {
				return errors.WithStack(err)
			}
			column := &schema.Column{
				Name:     columnName,
				Type:     dataType,
				Nullable: convertColumnNullable(isNullable),
				Default:  columnDefault,
				Comment:  columnComment.String,
			}
			columns = append(columns, column)
		}
		table.Columns = columns

		tables = append(tables, table)
	}
	s.Tables = tables

	return nil
}

func (s *Clickhouse) Info() (*schema.Driver, error) {
	var v string
	row := s.db.QueryRow(`SELECT version();`)
	if err := row.Scan(&v); err != nil {
		return nil, err
	}
	return &schema.Driver{
		Name:            "clickhouse",
		DatabaseVersion: v,
	}, nil
}

func convertColumnNullable(str string) bool {
	return str != "NO"
}

