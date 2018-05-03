package gpscp

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
)

func initializeFlags() {
	copyPath = flag.String("copy-path", "", "The absolute path of where the file will be copied on the specified hosts")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	fileToCopy = flag.String("file", "", "The absolute path of a file to be copied to the specified hosts")
	flag.Var(&hostNames, "hostname", "The name of a single host that will participate in the SCP session.  --hostname can be specified multiple times.")
	hostFile = flag.String("hostfile", "", "The absolute path of a file containing a list of hosts to participate in the SCP session")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
}

func DoInit() {
	gplog.InitializeLogging("gpscp", "")
	initializeFlags()
	InitializeSignalHandler(DoCleanup, "gpscp process", &wasTerminated)
}

func DoFlagValidation() {
	flag.Parse()
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	//SetLoggerVerbosity()

	globalCluster = cluster.NewCluster(nil)
}

func DoScp() {
}

func InitializeSignalHandler(cleanupFunc func(), procDesc string, termFlag *bool) {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for range signalChan {
			fmt.Println() // Add newline after "^C" is printed
			gplog.Warn("Received a termination signal, aborting %s", procDesc)
			*termFlag = true
			cleanupFunc()
			os.Exit(2)
		}
	}()
}

func DoTeardown() {
	errStr := ""
	if err := recover(); err != nil {
		errStr = fmt.Sprintf("%v", err)
	}
	if wasTerminated {
		CleanupGroup.Wait()
		return
	}
	if errStr != "" {
		fmt.Println(errStr)
	}
	errorCode := gplog.GetErrorCode()

	DoCleanup()

	if errorCode == 0 {
		gplog.Info("gpscp completed successfully")
	}
	os.Exit(errorCode)
}

func DoCleanup() {
	defer func() {
		if err := recover(); err != nil {
			gplog.Warn("Encountered error during cleanup: %v", err)
		}
		gplog.Verbose("Cleanup complete")
		CleanupGroup.Done()
	}()
	gplog.Verbose("Beginning cleanup")
	// Any cleanup happens here
}
