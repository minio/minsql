package server

import (
	"errors"

	"github.com/minio/minio/pkg/s3select/sql"
)

// GetTableName - gets the table name from the SQL query.
func GetTableName(s string) (table string, err error) {
	if s == "" {
		return "", errors.New("sql statement cannot be empty")
	}

	var selectAST sql.Select
	if err = sql.SQLParser.ParseString(s, &selectAST); err != nil {
		return
	}

	table = selectAST.From.Table.String()
	return table, nil
}
