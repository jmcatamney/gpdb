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
			It("Returns a slice containing one QueryTableAtts", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetTableAtts(connection, "foo")
				Expect(len(results)).Should(Equal(1))
				Expect(results[0].AttName).Should(Equal("i"))
				Expect(results[0].AttHasDef).Should(Equal(false))
				Expect(results[0].AttIsDropped).Should(Equal(false))
			})
		})
		Context("Table with multiple columns exists", func() {
			It("Returns a slice containing one QueryTableAtts per attribute", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetTableAtts(connection, "foo")
				Expect(len(results)).Should(Equal(2))
				Expect(results[0].AttName).Should(Equal("i"))
				Expect(results[0].AttTypName).Should(Equal("int"))
				Expect(results[1].AttName).Should(Equal("j"))
				Expect(results[1].AttTypName).Should(Equal("character varying(20)"))
			})
		})
		Context("Table with non-NULL attencoding column", func() {
			It("Returns a slice containing one QueryTableAtts with a non-NULL AttEncoding value", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowEncoded...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
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
			It("Returns a slice containing one QueryTableAtts with AttNotNull set to True", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowNotNull...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
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
	Describe("GetTableDefs", func() {
		BeforeEach(func() {
			connection, mock = testutils.CreateAndConnectMockDB()
		})
		header := []string{"adnum", "defval"}
		rowOne := []driver.Value{"1", "42"}
		rowTwo := []driver.Value{"2", "bar"}

		Context("Table with one column having a default value", func() {
			It("Returns a slice containing one QueryTableAtts", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOne...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetTableDefs(connection, "foo")
				Expect(len(results)).Should(Equal(1))
				Expect(results[0].AdNum).Should(Equal(1))
				Expect(results[0].DefVal).Should(Equal("42"))
			})
		})
		Context("Table with two columns having a default value", func() {
			It("Returns a slice containing one QueryTableAtts", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetTableDefs(connection, "foo")
				Expect(len(results)).Should(Equal(2))
				Expect(results[0].AdNum).Should(Equal(1))
				Expect(results[0].DefVal).Should(Equal("42"))
				Expect(results[1].AdNum).Should(Equal(2))
				Expect(results[1].DefVal).Should(Equal("bar"))
			})
		})
		Context("Table with no columns having default values", func() {
			It("Returns a slice containing one QueryTableAtts", func() {
				fakeResult := sqlmock.NewRows(header)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetTableDefs(connection, "foo")
				Expect(len(results)).Should(Equal(0))
			})
		})
	})
})
