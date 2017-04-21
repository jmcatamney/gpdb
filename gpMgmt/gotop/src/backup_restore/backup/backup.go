package backup

import (
	"backup_restore/utils"
	"flag"
	"fmt"
)

var (
	connection *utils.DBConn
	logger     *utils.Logger
)

var ( // Command-line flags
	dbname  = flag.String("dbname", "", "The database to be backed up")
	debug   = flag.Bool("debug", false, "Print verbose and debug log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
)

func DoInit() { // Handles setup that can be done before parsing flags
	logger = utils.InitializeLogging("gpbackup", "", utils.LOGINFO)
}

func DoValidation() {
	flag.Parse()
}

func DoSetup() { // Handles setup that must be done after parsing flags
	if *debug {
		logger.SetVerbosity(utils.LOGDEBUG)
	} else if *verbose {
		logger.SetVerbosity(utils.LOGVERBOSE)
	}
	connection = utils.NewDBConn(*dbname)
	connection.Connect()
}

func DoBackup() {
	logger.Info("Dump Key = %s", utils.CurrentTimestamp())
	logger.Info("Dump Database = %s", connection.DBName)
	logger.Info("Database Size = %s", connection.GetDBSize())

	metadataFilename := "/tmp/metadata.sql"
	logger.Info("Writing metadata to %s", metadataFilename)
	backupMetadata(metadataFilename)
	logger.Info("Metadata dump complete")
}

func backupMetadata(filename string) {
	metadataFile := utils.MustOpenFile(filename)
	connection.Begin()

	tables := GetAllUserTables(connection)
	logger.Verbose("Writing CREATE SCHEMA statements to metadata file")
	PrintCreateSchemaStatements(metadataFile, tables)
	logger.Verbose("Writing CREATE TABLE statements to metadata file")
	for _, table := range tables {
		columnDefs, tableDef := ConstructDefinitionsForTable(connection, table)
		PrintCreateTableStatement(metadataFile, table, columnDefs, tableDef)
	}

	logger.Verbose("Writing ADD CONSTRAINT statements to metadata file")
	allConstraints, allFkConstraints := ConstructConstraintsForAllTables(connection, tables)
	PrintConstraintStatements(metadataFile, allConstraints, allFkConstraints)

	connection.Commit()
}

func DoTeardown() {
	if r := recover(); r != nil {
		fmt.Println(r)
	}
	if connection != nil {
		connection.Close()
	}
	// TODO: Add logic for error codes based on whether we Abort()ed or not
}
