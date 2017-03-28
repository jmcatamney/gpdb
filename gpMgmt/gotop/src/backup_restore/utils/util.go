package utils

import (
	"fmt"
	"os"
	"time"
)

var (
	FPTimeNow = time.Now
	FPOsGetenv = os.Getenv
)

// Pass in printf()-style message and interpolation args, then end the command appropriately
func Abort(output ...interface{}) {
	errStr := ""
	if len(output) > 1 {
		errStr = fmt.Sprintf(output[0].(string) + "\n", output[1:]...)
	} else if len(output) == 1 {
		errStr = fmt.Sprintf(output[0].(string) + "\n")
	}
	panic(errStr)
}

func CheckError(err error) {
	if err != nil {
		Abort(err)
	}
}

func CurrentTimestamp() string {
	return FPTimeNow().Format("20060102150405")
}

func RecoverFromFailure() {
	if r := recover(); r != nil {
		fmt.Printf("[CRITICAL] %v\n", r) // TODO: Replace with logging command when we implement that
	}
}

// If the environment variable is set, return that, else return the default
func TryEnv(varname string, defval string) string {
	val := FPOsGetenv(varname)
	if val == "" {
		return defval
	}
	return val
}
