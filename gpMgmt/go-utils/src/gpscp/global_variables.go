package gpscp

import (
	"sync"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
 * Non-flag variables
 */
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
	copyPath   *string
	debug      *bool
	fileToCopy *string
	hostFile   *string
	hostNames  ArrayFlags
	quiet      *bool
	verbose    *bool
)

/*
 * Setter functions
 */

func SetCluster(cluster cluster.Cluster) {
	globalCluster = cluster
}
