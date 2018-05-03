package gpscp_test

import (
	"os"
	"os/user"
	"sort"

	"gpscp"

	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/operating"
	"github.com/greenplum-db/gp-common-go-libs/testhelper"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("gpscp tests", func() {
	masterSeg := cluster.SegConfig{ContentID: -1, Hostname: "localhost", DataDir: "/data/gpseg-1"}
	localSegOne := cluster.SegConfig{ContentID: 0, Hostname: "localhost", DataDir: "/data/gpseg0"}
	remoteSegOne := cluster.SegConfig{ContentID: 1, Hostname: "remotehost1", DataDir: "/data/gpseg1"}
	var (
		testCluster  cluster.Cluster
		testExecutor *testhelper.TestExecutor
	)

	BeforeEach(func() {
		operating.System.CurrentUser = func() (*user.User, error) { return &user.User{Username: "testUser", HomeDir: "testDir"}, nil }
		operating.System.Hostname = func() (string, error) { return "testHost", nil }
		testExecutor = &testhelper.TestExecutor{}
		testCluster = cluster.NewCluster([]cluster.SegConfig{masterSeg, localSegOne, remoteSegOne})
		testCluster.Executor = testExecutor
		gpscp.SetHostnames([]string{})
		gpscp.SetHostFile("")
		gpscp.SetCopyPath("")
		gpscp.SetFileToCopy("")
	})
	Describe("GetHostnames", func() {
		AfterEach(func() {
			gpscp.SetHostnames([]string{})
			gpscp.SetHostFile("")
			func() { operating.System.OpenFileRead = operating.OpenFileRead }()
		})
		It("successfully returns an array with multiples hostname passed by hostnames flag.", func() {
			gpscp.SetHostnames([]string{"foo", "bar"})
			hostnames := gpscp.GetHostnames()
			sort.Strings(hostnames)
			Expect(hostnames).To(Equal([]string{"bar", "foo"}))
		})
		It("successfully builds an array with multiple hostnames passed by hostfile flag.", func() {
			fileContents := []byte("foo\nbar\n")
			gpscp.SetHostFile("temp")
			r, w, _ := os.Pipe()
			operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return r, nil }
			w.Write(fileContents)
			w.Close()
			hostnames := gpscp.GetHostnames()
			sort.Strings(hostnames)
			Expect(hostnames).To(Equal([]string{"bar", "foo"}))
		})
		It("successfully returns an array with hostnames passed by both command line options.", func() {
			gpscp.SetHostnames([]string{"foo", "bar1"})
			fileContents := []byte("foo\nbar2\n")
			gpscp.SetHostFile("temp")
			r, w, _ := os.Pipe()
			operating.System.OpenFileRead = func(name string, flag int, perm os.FileMode) (operating.ReadCloserAt, error) { return r, nil }
			w.Write(fileContents)
			w.Close()
			hostnames := gpscp.GetHostnames()
			sort.Strings(hostnames)
			Expect(hostnames).To(Equal([]string{"bar1", "bar2", "foo"}))
		})
		//testExecutor.ClusterOutput = &cluster.RemoteOutput{
		//	NumErrors: 0,
		//}
		//Expect((*testExecutor).NumExecutions).To(Equal(1))
	})
	Describe("BuildCommands", func() {
		AfterEach(func() {
			gpscp.SetCopyPath("")
			gpscp.SetFileToCopy("")
		})
		It("successfully builds commands by replacing substitution character with hostnames.", func() {
			hostnames := []string{"foo", "bar"}
			gpscp.SetCopyPath("=:/temp")
			gpscp.SetFileToCopy("copy.txt")
			Expect(gpscp.BuildCommands(hostnames)).To(Equal(map[int][]string{0: []string{"scp", "copy.txt", "foo:/temp"}, 1: []string{"scp", "copy.txt", "bar:/temp"}}))
		})
		It("panics when a substitution character is not present in copy string.", func() {
			hostnames := []string{"foo", "bar"}
			gpscp.SetCopyPath(":/temp")
			gpscp.SetFileToCopy("copy.txt")
			defer testhelper.ShouldPanicWithMessage("No substitution character present in copy path.")
			gpscp.BuildCommands(hostnames)
		})
	})
})
