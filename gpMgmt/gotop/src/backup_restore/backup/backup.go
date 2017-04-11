package backup

import (
	"backup_restore/utils"
	"flag"
	"fmt"
	"os"
)

var connection *utils.DBConn

var dbname = flag.String("dbname", "", "The database to be backed up")

func DoValidation() {
	flag.Parse()
}

func DoSetup() {
	connection = utils.NewDBConn(*dbname)
	connection.Connect()
}

func DoBackup() {
	fmt.Println("The current time is", utils.CurrentTimestamp())
	fmt.Printf("Database %s is %s\n", connection.DBName, connection.GetDBSize())

	connection.Begin()

	tables := GetAllDumpableTables(connection)
	for _, table := range tables {
		tableAtts := GetTableAtts(connection, table.Oid)
		tableDefs := GetTableDefs(connection, table.Oid)
		PrintCreateTableStatement(os.Stdout, table.Tablename, tableAtts, tableDefs) // TODO: Change to write to file
	}
	for _, table := range tables {
		constraints := GetConstraints(connection, table.Oid)
		PrintAlterTableStatements(os.Stdout, table.Tablename, constraints) // TODO: Change to write to file
	}

	connection.Commit()
}

func DoTeardown() {
	if connection != nil {
		connection.Close()
	}
	// TODO: Add logic for error codes based on whether we Abort()ed or not
}
