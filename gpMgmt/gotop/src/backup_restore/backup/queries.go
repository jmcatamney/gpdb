package backup

import (
	"backup_restore/utils"
	"database/sql"
	"fmt"
)

func GetAllDumpableTables(connection *utils.DBConn) []utils.Table {
	query := `
SELECT ALLTABLES.oid, ALLTABLES.schemaname, ALLTABLES.tablename FROM

	(SELECT c.oid, n.nspname AS schemaname, c.relname AS tablename FROM pg_class c, pg_namespace n
	WHERE n.oid = c.relnamespace) as ALLTABLES,

	(SELECT n.nspname AS schemaname, c.relname AS tablename
	FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
	LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace
	WHERE c.relkind = 'r'::"char" AND c.oid > 16384 AND (c.relnamespace > 16384 or n.nspname = 'public')
	EXCEPT
	((SELECT x.schemaname, x.partitiontablename FROM
	(SELECT distinct schemaname, tablename, partitiontablename, partitionlevel FROM pg_partitions) as X,
	(SELECT schemaname, tablename maxtable, max(partitionlevel) maxlevel FROM pg_partitions group by (tablename, schemaname)) as Y
	WHERE x.schemaname = y.schemaname and x.tablename = Y.maxtable and x.partitionlevel != Y.maxlevel)
	UNION (SELECT distinct schemaname, tablename FROM pg_partitions))) as DATATABLES

WHERE ALLTABLES.schemaname = DATATABLES.schemaname and ALLTABLES.tablename = DATATABLES.tablename AND ALLTABLES.oid not in (select reloid from pg_exttable) AND ALLTABLES.schemaname NOT LIKE 'pg_temp_%' ORDER BY DATATABLES.schemaname, DATATABLES.tablename;`

	results := make([]utils.Table, 0)

	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryTableAtts struct {
	AttNum       int
	AttName      string
	AttNotNull   bool
	AttHasDef    bool
	AttIsDropped bool
	AttTypName   string
	AttEncoding  sql.NullString
}

func GetTableAtts(connection *utils.DBConn, oid uint32) []QueryTableAtts {
	query := fmt.Sprintf(`
SELECT a.attnum,
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
WHERE a.attrelid = %d
	AND a.attnum > 0::pg_catalog.int2
ORDER BY a.attrelid,
	a.attnum;`, oid)

	results := make([]QueryTableAtts, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryTableDefs struct {
	AdNum  int
	DefVal string
}

func GetTableDefs(connection *utils.DBConn, oid uint32) []QueryTableDefs {
	query := fmt.Sprintf(`
SELECT adnum,
	pg_catalog.pg_get_expr(adbin, adrelid) AS defval 
FROM pg_catalog.pg_attrdef
WHERE adrelid = %d;`, oid)

	results := make([]QueryTableDefs, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryPkFkUniqueConstraint struct {
	ConName string
	ConDef  string
}

func GetPkFkUniqueConstraints(connection *utils.DBConn, oid uint32) []QueryPkFkUniqueConstraint {
	/* The following query is not taken from pg_dump, as the pg_dump query gets a lot of information we
	 * don't need and is relatively slow due to several JOINS, the slowest of which is on pg_depend. This
	 * query is based on the queries underlying \d in psql, has roughly half the cost according to EXPLAIN,
	 * and gets us only the information we need.*/
	query := fmt.Sprintf(`
SELECT DISTINCT
	r.conname,
	pg_catalog.pg_get_constraintdef(r.oid, TRUE) AS condef
FROM pg_catalog.pg_constraint r
JOIN pg_catalog.pg_attribute a
	ON r.conrelid = a.attrelid
JOIN (SELECT
	unnest(conkey) AS un
FROM pg_constraint) AS keys
	ON a.attnum = keys.un
WHERE r.conrelid = %d
ORDER BY condef;
`, oid)

	results := make([]QueryPkFkUniqueConstraint, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}
