package backup_test

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var gpbackupPath = ""

// Helper function to execute gpbackup and return a session for stdout checking
func gpbackup() *gexec.Session {
	command := exec.Command(gpbackupPath)
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())
	<-session.Exited
	return session
}

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "gpbackup integration tests")
}

var _ = Describe("environment tests", func() {
	BeforeSuite(func() {
		var err error
		gpbackupPath, err = gexec.Build("backup_restore")
		Expect(err).ShouldNot(HaveOccurred())
		exec.Command("dropdb", "testdb").Run()
		err = exec.Command("createdb", "testdb").Run()
		if err != nil {
			Fail(fmt.Sprintf("%v", err))
		}
	})
	AfterSuite(func() {
		gexec.CleanupBuildArtifacts()
		exec.Command("dropdb", "testdb").Run()
	})

	It("Succeeds when PGDATABASE is set", func() {
		oldPgDatabase := os.Getenv("PGDATABASE")
		os.Setenv("PGDATABASE", "testdb")
		defer os.Setenv("PGDATABASE", oldPgDatabase)

		session := gpbackup()
		Expect(session.Out).Should(gbytes.Say("The current time is"))
	})
	It("Fails when PGDATABASE is unset", func() {
		oldPgDatabase := os.Getenv("PGDATABASE")
		os.Setenv("PGDATABASE", "")
		defer os.Setenv("PGDATABASE", oldPgDatabase)

		session := gpbackup()
		Expect(session.Out).Should(gbytes.Say("CRITICAL"))
	})
})
