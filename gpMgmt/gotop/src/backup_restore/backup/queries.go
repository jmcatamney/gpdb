package backup

import (
	"backup_restore/utils"
	"database/sql"
	"fmt"
)

type QueryTableAtts struct {
	AttNum int
	AttName string
	AttNotNull bool
	AttHasDef bool
	AttIsDropped bool
	AttTypName string
	AttEncoding sql.NullString
}

func GetTableAtts(connection *utils.DBConn, tablename string) []QueryTableAtts {
	query := `SELECT a.attnum,
	a.attname,
	a.attnotnull,
	a.atthasdef,
	a.attisdropped,
	pg_catalog.format_type(t.oid,a.atttypmod) AS atttypname,
	pg_catalog.array_to_string(e.attoptions, ',') AS attencoding
FROM pg_catalog.pg_attribute a
	LEFT JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
	LEFT OUTER JOIN pg_catalog.pg_attribute_encoding e ON e.attrelid = a.attrelid
	AND e.attnum = a.attnum
WHERE a.attrelid = %s
	AND a.attnum > 0::pg_catalog.int2
ORDER BY a.attrelid,
	a.attnum;`

	table := fmt.Sprintf("'%s'::regclass::pg_catalog.oid", tablename); // TODO: Replace with oid instead of cast at some point for performance
	query = fmt.Sprintf(query, table)

	results := make([]QueryTableAtts, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryTableDefs struct {
	AdNum int
	DefVal string
}

func GetTableDefs(connection *utils.DBConn, tablename string) []QueryTableDefs {
	query := `SELECT adnum,
	pg_catalog.pg_get_expr(adbin, adrelid) AS defval 
FROM pg_catalog.pg_attrdef
WHERE adrelid = %s;`

	table := fmt.Sprintf("'%s'::regclass::pg_catalog.oid", tablename); // TODO: Replace with oid instead of cast at some point for performance
	query = fmt.Sprintf(query, table)

	results := make([]QueryTableDefs, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
