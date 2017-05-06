package backup

import (
	"backup_restore/utils"
	"database/sql"
	"fmt"
	"strings"
)

func GetAllUserTables(connection *utils.DBConn) []utils.Table {
	query := `
SELECT
	c.oid,
  n.nspname AS schemaname,
  c.relname AS tablename
FROM pg_class c
LEFT JOIN pg_partition_rule pr
  ON c.oid = pr.parchildrelid
LEFT JOIN pg_partition p
  ON pr.paroid = p.oid
LEFT JOIN pg_namespace n
  ON c.relnamespace = n.oid
WHERE relkind = 'r'
AND c.oid NOT IN (SELECT
  p.parchildrelid
FROM pg_partition_rule p
LEFT
JOIN pg_exttable e
  ON p.parchildrelid = e.reloid
WHERE e.reloid IS NULL)
AND (c.relnamespace > 16384
OR n.nspname = 'public')
ORDER BY schemaname, tablename;`

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

func GetTableAttributes(connection *utils.DBConn, oid uint32) []QueryTableAtts {
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

func GetTableDefaults(connection *utils.DBConn, oid uint32) []QueryTableDefs {
	query := fmt.Sprintf(`
SELECT adnum,
	pg_catalog.pg_get_expr(adbin, adrelid) AS defval 
FROM pg_catalog.pg_attrdef
WHERE adrelid = %d
ORDER BY adrelid,
	adnum;`, oid)

	results := make([]QueryTableDefs, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryConstraint struct {
	ConName string
	ConType string
	ConDef  string
}

func GetConstraints(connection *utils.DBConn, oid uint32) []QueryConstraint {
	/* The following query is not taken from pg_dump, as the pg_dump query gets a lot of information we
	 * don't need and is relatively slow due to several JOINS, the slowest of which is on pg_depend. This
	 * query is based on the queries underlying \d in psql, has roughly half the cost according to EXPLAIN,
	 * and gets us only the information we need.*/
	query := fmt.Sprintf(`
SELECT
	conname,
	contype,
	pg_catalog.pg_get_constraintdef(oid, TRUE) AS condef
FROM pg_catalog.pg_constraint
WHERE conrelid = %d;
`, oid)

	results := make([]QueryConstraint, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	return results
}

type QueryDistPolicy struct {
	AttName string
}

func GetDistributionPolicy(connection *utils.DBConn, oid uint32) string {
	query := fmt.Sprintf(`
SELECT a.attname
FROM pg_attribute a
JOIN (
	SELECT
		unnest(attrnums) AS attnum, 
		localoid
	FROM gp_distribution_policy
) p
ON (p.localoid,p.attnum) = (a.attrelid,a.attnum)
WHERE a.attrelid = %d;`, oid)
	results := make([]QueryDistPolicy, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	if len(results) == 0 {
		return "DISTRIBUTED RANDOMLY"
	} else {
		distCols := make([]string, 0)
		for _, dist := range results {
			distCols = append(distCols, dist.AttName)
		}
		return fmt.Sprintf("DISTRIBUTED BY (%s)", strings.Join(distCols, ", "))
	}
}

type QueryPartDef struct {
	PartitionDef string `db:"pg_get_partition_def"`
}

func GetPartitionDefinition(connection *utils.DBConn, oid uint32) string {
	query := fmt.Sprintf("SELECT * from pg_get_partition_def(%d, true, true) where pg_get_partition_def IS NOT NULL", oid)
	results := make([]QueryPartDef, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	if len(results) == 1 {
		return results[0].PartitionDef
	} else if len(results) > 1 {
		utils.Abort("Too many rows returned from query to get partition definition: got %d rows, expected 1 row", len(results))
	}
	return ""
}

type QueryStorageOptions struct {
	StorageOptions sql.NullString
}

func GetStorageOptions(connection *utils.DBConn, oid uint32) string {
	query := fmt.Sprintf(`
SELECT array_to_string(reloptions, ', ') as storageoptions
FROM pg_class
WHERE oid = %d AND reloptions IS NOT NULL;`, oid)
	results := make([]QueryStorageOptions, 0)
	err := connection.Select(&results, query)
	utils.CheckError(err)
	if len(results) == 1 {
		return results[0].StorageOptions.String
	} else if len(results) > 1 {
		utils.Abort("Too many rows returned from query to get storage options: got %d rows, expected 1 row", len(results))
	}
	return ""
}
