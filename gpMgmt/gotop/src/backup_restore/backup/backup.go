package backup

import (
	"backup_restore/utils"
	"flag"
	"fmt"
)

var Connection *utils.DBConn

var dbname = flag.String("dbname", "", "The database to be backed up")

func DoValidation() {
	flag.Parse()
}

func DoSetup() {
	Connection = utils.NewDBConn(*dbname)
	Connection.Connect()
}

func DoBackup() {
	fmt.Println("The current time is", utils.CurrentTimestamp())

	pgTablesArray := make([]struct {
		Schemaname string;
		Tablename string
	}, 0)
	err := Connection.Select(&pgTablesArray, "SELECT schemaname,tablename FROM pg_tables ORDER BY schemaname, tablename")
	utils.CheckError(err)
	for _, datum := range pgTablesArray {
		fmt.Printf("%s.%s\n", datum.Schemaname, datum.Tablename)
	}
}

func DoTeardown() {
	if Connection != nil {
		Connection.Close()
	}
}
