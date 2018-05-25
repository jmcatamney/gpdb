package main

import (
	"github.com/greenplum-db/gp-common-go-libs/cluster"
)

/*
 * This file contains global variables and setter functions for those variables
 * used in testing.
 */

/*
 * Non-flag variables
 */

/*
 * Setter functions
 */

func SetCluster(cluster cluster.Cluster) {
	globalCluster = cluster
}

func SetHostnames(hosts ArrayFlags) {
	hostNames = hosts
}

func SetCopyPath(path string) {
	copyPath = path
}

func SetFileToCopy(file string) {
	fileToCopy = file
}

func SetHostFile(file string) {
	hostFile = &file
}
