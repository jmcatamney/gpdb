package gpscp

/*
 * This file contains functions and structs relating to flag parsing.
 */

import (
	"flag"
	"strings"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/pkg/errors"
)

/*
 * Functions for validating whether flags are set and in what combination
 */

func FlagIsSet(f *flag.Flag) bool {
	return (*f).Value.String() != (*f).DefValue
}

// Each flag passed to this function must be set
func CheckMandatoryFlags(flagNames ...string) {
	for _, name := range flagNames {
		f := flag.Lookup(name)
		if f == nil || !FlagIsSet(f) {
			gplog.Fatal(errors.Errorf("Flag %s must be set", name), "")
		}
	}
}

// At most one of the flags passed to this function may be set
func CheckExclusiveFlags(flagNames ...string) {
	numSet := 0
	for _, name := range flagNames {
		f := flag.Lookup(name)
		if f != nil && FlagIsSet(f) {
			numSet++
		}
	}
	if numSet > 1 {
		gplog.Fatal(errors.Errorf("The following flags may not be specified together: %s", strings.Join(flagNames, ", ")), "")
	}
}

type ArrayFlags []string

func (i *ArrayFlags) String() string {
	return strings.Join(*i, ", ")
}

func (i *ArrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

/*
 * Functions for validating flag values
 */

func ValidateAbsolutePath(flag string, path string) {
	if len(path) > 0 && string(path[0]) != "/" {
		gplog.Fatal(errors.Errorf("Value of %s must be an absolute path.", flag), "")
	}
}
