package backup_test

import (
	"backup_restore/backup"
	"backup_restore/testutils"
	"backup_restore/utils"
	"database/sql/driver"
	"errors"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var connection *utils.DBConn
var mock sqlmock.Sqlmock

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "queries.go unit tests")
}

func matchPrimaryUnique(result backup.QueryPrimaryUniqueConstraint, primary bool, unique bool) {
	Expect(result.IsPrimary).Should(Equal(primary))
	Expect(result.IsUnique).Should(Equal(unique))
}

var _ = Describe("backup/queries tests", func() {
	Describe("GetTableAtts", func() {
		BeforeEach(func() {
			connection, mock = testutils.CreateAndConnectMockDB()
		})
		header := []string{"attname", "attnotnull", "atthasdef", "attisdropped", "atttypname", "attencoding"}
		rowOne := []driver.Value{"i", "f", "f", "f", "int", nil}
		rowTwo := []driver.Value{"j", "f", "f", "f", "character varying(20)", nil}
		rowEncoded := []driver.Value{"j", "f", "f", "f", "character varying(20)", "compresstype=zlib, blocksize=65536"}
		rowNotNull := []driver.Value{"j", "t", "f", "f", "character varying(20)", nil}

		It("returns a slice for a table with one column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).Should(Equal(1))
			Expect(results[0].AttName).Should(Equal("i"))
			Expect(results[0].AttHasDef).Should(Equal(false))
			Expect(results[0].AttIsDropped).Should(Equal(false))
		})
		It("returns a slice for a table with two columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).Should(Equal(2))
			Expect(results[0].AttName).Should(Equal("i"))
			Expect(results[0].AttTypName).Should(Equal("int"))
			Expect(results[1].AttName).Should(Equal("j"))
			Expect(results[1].AttTypName).Should(Equal("character varying(20)"))
		})
		It("returns a slice for a table with one NOT NULL column with ENCODING", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowEncoded...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).Should(Equal(2))
			Expect(results[0].AttName).Should(Equal("i"))
			Expect(results[0].AttEncoding.Valid).Should(Equal(false))
			Expect(results[1].AttName).Should(Equal("j"))
			Expect(results[1].AttEncoding.Valid).Should(Equal(true))
			Expect(results[1].AttEncoding.String).Should(Equal("compresstype=zlib, blocksize=65536"))
		})
		It("returns a slice for a table with one NOT NULL column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowNotNull...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).Should(Equal(2))
			Expect(results[0].AttName).Should(Equal("i"))
			Expect(results[0].AttEncoding.Valid).Should(Equal(false))
			Expect(results[1].AttName).Should(Equal("j"))
			Expect(results[1].AttNotNull).Should(Equal(true))
		})
		It("panics when table does not exist", func() {
			mock.ExpectQuery("SELECT (.*)").WillReturnError(errors.New("relation \"foo\" does not exist"))
			defer testutils.ShouldPanicWithMessage("relation \"foo\" does not exist")
			backup.GetTableAtts(connection, 0)
		})
	})
	Describe("GetTableDefs", func() {
		BeforeEach(func() {
			connection, mock = testutils.CreateAndConnectMockDB()
		})
		header := []string{"adnum", "defval"}
		rowOne := []driver.Value{"1", "42"}
		rowTwo := []driver.Value{"2", "bar"}

		It("returns a slice for a table with one column having a default value", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefs(connection, 0)
			Expect(len(results)).Should(Equal(1))
			Expect(results[0].AdNum).Should(Equal(1))
			Expect(results[0].DefVal).Should(Equal("42"))
		})
		It("returns a slice for a table with two columns having default values", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefs(connection, 0)
			Expect(len(results)).Should(Equal(2))
			Expect(results[0].AdNum).Should(Equal(1))
			Expect(results[0].DefVal).Should(Equal("42"))
			Expect(results[1].AdNum).Should(Equal(2))
			Expect(results[1].DefVal).Should(Equal("bar"))
		})
		It("returns a slice for a table with no columns having default values", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefs(connection, 0)
			Expect(len(results)).Should(Equal(0))
		})
	})
	Describe("GetPrimaryUniqueConstraints", func() {
		BeforeEach(func() {
			connection, mock = testutils.CreateAndConnectMockDB()
		})
		header := []string{"attname", "isprimary", "isunique"}
		rowOne := []driver.Value{"i", "f", "f"}
		rowTwo := []driver.Value{"j", "f", "f"}
		rowOneUnique := []driver.Value{"i", "f", "t"}
		rowTwoUnique := []driver.Value{"j", "f", "t"}
		rowOnePrimary := []driver.Value{"i", "t", "t"}
		rowTwoPrimary := []driver.Value{"j", "t", "t"}
		rowThreePrimary := []driver.Value{"k", "t", "t"}

		It("returns a slice for a table with no UNIQUE or PRIMARY KEY columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(2))
			matchPrimaryUnique(results[0], false, false)
			matchPrimaryUnique(results[1], false, false)
		})
		It("returns a slice for a table with one UNIQUE column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwoUnique...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(2))
			matchPrimaryUnique(results[0], false, false)
			matchPrimaryUnique(results[1], false, true)
		})
		It("returns a slice for a table with two UNIQUE columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowTwoUnique...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(2))
			matchPrimaryUnique(results[0], false, true)
			matchPrimaryUnique(results[1], false, true)
		})
		It("returns a slice for a table with one PRIMARY KEY column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOnePrimary...).AddRow(rowTwoUnique...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(2))
			matchPrimaryUnique(results[0], true, true)
			matchPrimaryUnique(results[1], false, true)
		})
		It("returns a slice for a table with two PRIMARY KEY columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOnePrimary...).AddRow(rowTwoPrimary...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(2))
			matchPrimaryUnique(results[0], true, true)
			matchPrimaryUnique(results[1], true, true)
		})
		It("returns a slice for a table with PRIMARY KEY column and one UNIQUE column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOnePrimary...).AddRow(rowTwoUnique...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(2))
			matchPrimaryUnique(results[0], true, true)
			matchPrimaryUnique(results[1], false, true)
		})
		It("returns a slice for a table with two PRIMARY KEY columns and one UNIQUE column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOnePrimary...).AddRow(rowTwoUnique...).AddRow(rowThreePrimary...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetPrimaryUniqueConstraints(connection, 0)
			Expect(len(results)).Should(Equal(3))
			matchPrimaryUnique(results[0], true, true)
			matchPrimaryUnique(results[1], false, true)
			matchPrimaryUnique(results[2], true, true)
		})
	})
})
