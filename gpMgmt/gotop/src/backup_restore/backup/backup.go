package backup

import (
	"backup_restore/utils"
	"fmt"
	"os"
)

var connection *utils.DBConn

func SetUp() {
	connection = utils.NewDBConn("")
	err := connection.Connect()
	utils.CheckError(err)
}

func DoBackup() {
	fmt.Println("The current time is", utils.CurrentTimestamp())
	rows, err := connection.GetRows("select schemaname,tablename from pg_tables")
	utils.CheckError(err)
	for _, datum := range rows {
		fmt.Printf("The schema for table %s is %s\n", datum[1], datum[0])
	}
}

func TearDown() {
	fmt.Println("Got to tearDown")
	os.Exit(0)
}
