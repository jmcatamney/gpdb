package backup_test

import (
	"backup_restore/backup"
	"backup_restore/utils"
	"backup_restore/testutils"
	"database/sql/driver"
	"errors"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock"
)

var connection *utils.DBConn
var mock sqlmock.Sqlmock

func TestQueries(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "queries.go unit tests")
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

		Context("Table with one column exists", func() {
			It("Returns a slice containing one TableAtts", func() {
				tableOneColumn := sqlmock.NewRows(header).AddRow(rowOne...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableOneColumn)
				results := backup.GetTableAtts(connection, "foo")
				Expect(len(results)).Should(Equal(1))
				Expect(results[0].AttName).Should(Equal("i"))
				Expect(results[0].AttHasDef).Should(Equal(false))
				Expect(results[0].AttIsDropped).Should(Equal(false))
			})
		})
		Context("Table with multiple columns exists", func() {
			It("Returns a slice containing one TableAtts per attribute", func() {
				tableTwoColumns := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableTwoColumns)
				results := backup.GetTableAtts(connection, "foo")
				Expect(len(results)).Should(Equal(2))
				Expect(results[0].AttName).Should(Equal("i"))
				Expect(results[0].AttTypName).Should(Equal("int"))
				Expect(results[1].AttName).Should(Equal("j"))
				Expect(results[1].AttTypName).Should(Equal("character varying(20)"))
			})
		})
		Context("Table with non-NULL attencoding column", func() {
			It("Returns a slice containing one TableAtts with a non-NULL AttEncoding value", func() {
				tableEncoded := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowEncoded...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableEncoded)
				results := backup.GetTableAtts(connection, "foo")
				Expect(len(results)).Should(Equal(2))
				Expect(results[0].AttName).Should(Equal("i"))
				Expect(results[0].AttEncoding.Valid).Should(Equal(false))
				Expect(results[1].AttName).Should(Equal("j"))
				Expect(results[1].AttEncoding.Valid).Should(Equal(true))
				Expect(results[1].AttEncoding.String).Should(Equal("compresstype=zlib, blocksize=65536"))
			})
		})
		Context("Table with NOT NULL column", func() {
			It("Returns a slice containing one TableAtts with AttNotNull set to True", func() {
				tableEncoded := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowNotNull...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(tableEncoded)
				results := backup.GetTableAtts(connection, "foo")
				Expect(len(results)).Should(Equal(2))
				Expect(results[0].AttName).Should(Equal("i"))
				Expect(results[0].AttEncoding.Valid).Should(Equal(false))
				Expect(results[1].AttName).Should(Equal("j"))
				Expect(results[1].AttNotNull).Should(Equal(true))
			})
		})
		Context("Table does not exist", func() {
			It("Panics", func() {
				mock.ExpectQuery("SELECT (.*)").WillReturnError(errors.New("relation \"foo\" does not exist"))
				defer testutils.ShouldPanicWithMessage("relation \"foo\" does not exist")
				backup.GetTableAtts(connection, "foo")
			})
		})
	})
})
