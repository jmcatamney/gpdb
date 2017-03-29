package backup_test

import (
	. "backup_restore/utils"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"os"
	"os/exec"
	"testing"
)

var gpbackupPath = "./gpbackup"

// Helper function to execute gpbackup and return a session for stdout checking
func gpbackup() *gexec.Session {
	command := exec.Command(gpbackupPath)
	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ShouldNot(HaveOccurred())
	return session
}

func TestBackup(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Backup Suite")
}

var _ = Describe("Backup", func() {
	BeforeSuite(func() {
		var err error
		gpbackupPath, err = gexec.Build("backup_restore")
		Expect(err).ShouldNot(HaveOccurred())
	})
	AfterSuite(func() {
		gexec.CleanupBuildArtifacts()
	})

	It("Succeeds when PGDATABASE is set", func() {
		session := gpbackup()
		Eventually(session.Out).Should(gbytes.Say("The current time is"))
	})
	It("Fails when PGDATABASE is unset", func() {
		FPOsGetenv = func(varname string) (string) {
			if varname == "PGDATABASE" {
				return "nonexistent"
			} else {
				return os.Getenv(varname)
			}
		}

		session := gpbackup()
		Eventually(session.Out).Should(gbytes.Say("CRITICAL"))
	})
})
