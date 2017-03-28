package main

import (
	. "backup_restore/backup"
	"backup_restore/utils"
)

func main() {
	defer TearDown()
	defer utils.RecoverFromFailure()
	SetUp()
	DoBackup()
}
