package backup

import (
	"backup_restore/utils"
	"fmt"
	"os"
)

var connection *utils.DBConn

func SetUp() {
	connection = utils.NewDBConn("")
	connection.Connect()
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
	err := connection.Select(&pgTablesArray, "select schemaname,tablename from pg_tables limit 2")
	utils.CheckError(err)
	for i, datum := range pgTablesArray {
		fmt.Printf("%d: The schema for table %s is %s\n", i, datum.Schemaname, datum.Tablename)
	}

	err = connection.Select(&barArray, "select * from bar")
	utils.CheckError(err)
	for _, datum := range barArray {
		fmt.Printf("Item: %d\n", datum.J)
	}
}

func TearDown() {
	fmt.Println("Got to tearDown")
	os.Exit(0)
}
