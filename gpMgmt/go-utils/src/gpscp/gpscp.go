// +build gpscp

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/greenplum-db/gp-common-go-libs/iohelper"
	"github.com/pkg/errors"
)

var (
	globalCluster cluster.Cluster
	wasTerminated bool

	/*
	 * Used for synchronizing DoCleanup.  In DoInit() we increment the group
	 * and then wait for at least one DoCleanup to finish, either in DoTeardown
	 * or the signal handler.
	 */
	CleanupGroup *sync.WaitGroup
)

/*
 * Command-line flags
 */
var (
	debug          *bool
	hostFile       *string
	hostNames      ArrayFlags
	quiet          *bool
	verbose        *bool
	perSegmentCopy *bool
	copyPath       string
	fileToCopy     string
)

func initializeFlags() {
	perSegmentCopy = flag.Bool("per-segment-copy", false, "Copy file to each segment.")
	debug = flag.Bool("debug", false, "Print verbose and debug log messages")
	flag.Var(&hostNames, "hostname", "The name of a single host that will participate in the SCP session.  --hostname can be specified multiple times.")
	hostFile = flag.String("hostfile", "", "The absolute path of a file containing a list of hosts to participate in the SCP session")
	quiet = flag.Bool("quiet", false, "Suppress non-warning, non-error log messages")
	verbose = flag.Bool("verbose", false, "Print verbose log messages")
}

func initializeCommandLineArgs() {
	args := flag.Args()
	if len(args) != 2 {
		fmt.Println("Usage: gpscp [local file] [remote copy path]")
		os.Exit(1)
	}
	fileToCopy = args[0]
	copyPath = args[1]
}

func main() {
	defer DoTeardown()
	DoInit()
	DoFlagValidation()
	DoSetup()
	DoScp()
}

func DoInit() {
	CleanupGroup = &sync.WaitGroup{}
	CleanupGroup.Add(1)
	gplog.InitializeLogging("gpscp", "")
	initializeFlags()
	InitializeSignalHandler(DoCleanup, "gpscp process", &wasTerminated)
}

func DoFlagValidation() {
	flag.Parse()
	CheckExclusiveFlags("hostFile", "hostNames")
}

func SetLoggerVerbosity() {
	if *quiet {
		gplog.SetVerbosity(gplog.LOGERROR)
	} else if *debug {
		gplog.SetVerbosity(gplog.LOGDEBUG)
	} else if *verbose {
		gplog.SetVerbosity(gplog.LOGVERBOSE)
	}
}

// This function handles setup that must be done after parsing flags.
func DoSetup() {
	SetLoggerVerbosity()
	initializeCommandLineArgs()

	globalCluster = cluster.NewCluster(nil)
	if *hostFile != "" {
		hostNames = iohelper.MustReadLinesFromFile(*hostFile)
	}
	if len(hostNames) == 0 {
		gplog.Fatal(errors.Errorf("No hostnames specified.  Use the --hostfile or --hostname flag to specify hostnames."), "")
	}
}

func DoScp() {
	hostNames = UniquifyHostnames(hostNames)
	commands := BuildCommands(hostNames)
	remoteOutput := globalCluster.ExecuteClusterCommand(cluster.ON_HOSTS, commands)
	globalCluster.CheckClusterError(remoteOutput, "Unable to execute copy", func(contentID int) string {
		return "Error executing copy"
	})
}

func UniquifyHostnames(hostnames []string) []string {
	oldNames := make(map[string]bool, 0)
	for _, hostname := range hostnames {
		oldNames[hostname] = true
	}
	newNames := make([]string, 0)
	for hostname := range oldNames {
		newNames = append(newNames, hostname)
	}
	sort.Strings(newNames)
	return newNames
}

func BuildCommands(hostnames []string) map[int][]string {
	fmt.Println("CopyPath:", copyPath)
	if !strings.Contains(copyPath, "=") {
		gplog.Fatal(errors.Errorf("No substitution character present in copy path."), "")
	}
	commands := make(map[int][]string, 0)
	index := 0
	for _, hostname := range hostnames {
		filePath := strings.Replace(copyPath, "=", hostname, 1)
		commands[index] = []string{"scp", fileToCopy, filePath}
		index++
	}
	return commands
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
}
