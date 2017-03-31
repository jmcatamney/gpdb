package backup

import (
	"backup_restore/utils"
	"flag"
	"fmt"
	"os"
)

var Connection *utils.DBConn

var dbname = flag.String("dbname", "", "The database to be backed up")

func DoValidation() {
	flag.Parse()
}

func DoSetup() {
	fmt.Println("Using database", *dbname)
	Connection = utils.NewDBConn(*dbname)
	Connection.Connect()
}

func DoBackup() {
	fmt.Println("The current time is", utils.CurrentTimestamp())
	barArray := make([]struct {
		J int
	}, 0)

	pgTablesArray := make([]struct {
		Schemaname string;
		Tablename string
	}, 0)
	err := Connection.Select(&pgTablesArray, "select schemaname,tablename from pg_tables limit 2")
	utils.CheckError(err)
	for i, datum := range pgTablesArray {
		fmt.Printf("%d: The schema for table %s is %s\n", i, datum.Schemaname, datum.Tablename)
	}

	err = Connection.Select(&barArray, "select * from bar")
	utils.CheckError(err)
	for _, datum := range barArray {
		fmt.Printf("Item: %d\n", datum.J)
	}
}

func DoTeardown() {
	//Connection.Conn.Close()
	os.Exit(0)
}
