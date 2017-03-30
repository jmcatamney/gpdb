package utils

import (
	"fmt"
	"os"
	"os/user"
	"strconv"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

type DBConn struct {
	Conn   *sqlx.DB
	User   string
	DBName string
	Host   string
	Port   int
}

type Table struct {
	Schema string
	Table  string
}

func NewDBConn(dbname string) *DBConn {
	username := ""
	host := ""
	port := 0

	default_user, _ := user.Current()
	default_host, _ := os.Hostname()
	username = TryEnv("PGUSER", default_user.Username)
	dbname = TryEnv("PGDATABASE", "")
	if dbname == "" {
		Abort("No database provided and PGDATABASE not set")
	}
	host = TryEnv("PGHOST", default_host)
	port, _ = strconv.Atoi(TryEnv("PGPORT", "5432"))

	return &DBConn{
		Conn:   nil,
		User:   username,
		DBName: dbname,
		Host:   host,
		Port:   port,
	}
}

func (dbconn *DBConn) Connect() {
	connStr := fmt.Sprintf("user=%s dbname=%s host=%s port=%d sslmode=disable", dbconn.User, dbconn.DBName, dbconn.Host, dbconn.Port)
	var err error
	dbconn.Conn, err = sqlx.Connect("postgres", connStr)
	if dbconn.Conn == nil {
		Abort("Cannot make connection to DB: %v", err)
	}
	CheckError(err)
	err = dbconn.Conn.Ping()
	CheckError(err)
}

func (dbconn *DBConn) Select(dest interface{}, query string) error {
	return dbconn.Conn.Select(dest, query)
}

/*func (dbconn *DBConn) GetRows(query string) ([][]interface{}, error) {
	rows, err := dbconn.Conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results [][]interface{}
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	vals := make([]interface{}, len(cols))

	for rows.Next() {
		row := make([]interface{}, len(cols))
		for i, _ := range cols {
			vals[i] = &row[i]
		}
		if err = rows.Scan(vals...); err != nil {
			return nil, err
		}
		for i, datum := range row {
			test, ok := datum.([]byte)
			if ok {
				row[i] = string(test)
			}
		}
		results = append(results, row)
	}
	return results, nil
}*/
