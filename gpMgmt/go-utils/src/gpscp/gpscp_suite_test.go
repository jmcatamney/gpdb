package gpscp_test

import (
	"testing"

	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var (
	stdout  *gbytes.Buffer
	stderr  *gbytes.Buffer
	logfile *gbytes.Buffer
	buffer  = gbytes.NewBuffer()
)

func TestGpScp(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gpscp tests")
}

var _ = BeforeSuite(func() {
	stdout, stderr, logfile = testhelper.SetupTestLogger()
})

var _ = BeforeEach(func() {
	buffer = gbytes.NewBuffer()
})
