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
			Expect(len(results)).To(Equal(1))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttHasDef).To(Equal(false))
			Expect(results[0].AttIsDropped).To(Equal(false))
		})
		It("returns a slice for a table with two columns", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttTypName).To(Equal("int"))
			Expect(results[1].AttName).To(Equal("j"))
			Expect(results[1].AttTypName).To(Equal("character varying(20)"))
		})
		It("returns a slice for a table with one NOT NULL column with ENCODING", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowEncoded...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttEncoding.Valid).To(Equal(false))
			Expect(results[1].AttName).To(Equal("j"))
			Expect(results[1].AttEncoding.Valid).To(Equal(true))
			Expect(results[1].AttEncoding.String).To(Equal("compresstype=zlib, blocksize=65536"))
		})
		It("returns a slice for a table with one NOT NULL column", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowNotNull...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableAtts(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AttName).To(Equal("i"))
			Expect(results[0].AttEncoding.Valid).To(Equal(false))
			Expect(results[1].AttName).To(Equal("j"))
			Expect(results[1].AttNotNull).To(Equal(true))
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
			Expect(len(results)).To(Equal(1))
			Expect(results[0].AdNum).To(Equal(1))
			Expect(results[0].DefVal).To(Equal("42"))
		})
		It("returns a slice for a table with two columns having default values", func() {
			fakeResult := sqlmock.NewRows(header).AddRow(rowOne...).AddRow(rowTwo...)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefs(connection, 0)
			Expect(len(results)).To(Equal(2))
			Expect(results[0].AdNum).To(Equal(1))
			Expect(results[0].DefVal).To(Equal("42"))
			Expect(results[1].AdNum).To(Equal(2))
			Expect(results[1].DefVal).To(Equal("bar"))
		})
		It("returns a slice for a table with no columns having default values", func() {
			fakeResult := sqlmock.NewRows(header)
			mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
			results := backup.GetTableDefs(connection, 0)
			Expect(len(results)).To(Equal(0))
		})
	})
	Describe("GetPkFkUniqueConstraints", func() {
		BeforeEach(func() {
			connection, mock = testutils.CreateAndConnectMockDB()
		})
		header := []string{"conname", "condef"}
		rowOneUnique := []driver.Value{"tablename_i_uniq", "UNIQUE (i)"}
		rowTwoUnique := []driver.Value{"tablename_j_uniq", "UNIQUE (j)"}
		rowPrimarySingle := []driver.Value{"tablename_pkey", "PRIMARY KEY (i)"}
		rowPrimaryComposite := []driver.Value{"tablename_pkey", "PRIMARY KEY (i, j)"}
		rowOneForeign := []driver.Value{"tablename_i_fkey", "FOREIGN KEY (i) REFERENCES other_tablename(a)"}
		rowTwoForeign := []driver.Value{"tablename_j_fkey", "FOREIGN KEY (j) REFERENCES other_tablename(b)"}

		Context("No constraints", func() {
			It("returns a slice for a table with no UNIQUE or PRIMARY KEY columns", func() {
				fakeResult := sqlmock.NewRows(header)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(0))
			})
		})
		Context("Columns with one constraint", func() {
			It("returns a slice for a table with one UNIQUE column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("tablename_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
			})
			It("returns a slice for a table with two UNIQUE columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowTwoUnique...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("tablename_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
				Expect(results[1].ConName).To(Equal("tablename_j_uniq"))
				Expect(results[1].ConDef).To(Equal("UNIQUE (j)"))
			})
			It("returns a slice for a table with a PRIMARY KEY on one column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowPrimarySingle...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("tablename_pkey"))
				Expect(results[0].ConDef).To(Equal("PRIMARY KEY (i)"))
			})
			It("returns a slice for a table with a composite PRIMARY KEY on two columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowPrimaryComposite...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("tablename_pkey"))
				Expect(results[0].ConDef).To(Equal("PRIMARY KEY (i, j)"))
			})
			It("returns a slice for a table with one FOREIGN KEY column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneForeign...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(1))
				Expect(results[0].ConName).To(Equal("tablename_i_fkey"))
				Expect(results[0].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
			})
			It("returns a slice for a table with two FOREIGN KEY columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneForeign...).AddRow(rowTwoForeign...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("tablename_i_fkey"))
				Expect(results[0].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
				Expect(results[1].ConName).To(Equal("tablename_j_fkey"))
				Expect(results[1].ConDef).To(Equal("FOREIGN KEY (j) REFERENCES other_tablename(b)"))
			})
		})
		Context("Columns with multiple constraints", func() {
			It("returns a slice for a table with a single UNIQUE FOREIGN KEY column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowOneForeign...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("tablename_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
				Expect(results[1].ConName).To(Equal("tablename_i_fkey"))
				Expect(results[1].ConDef).To(Equal("FOREIGN KEY (i) REFERENCES other_tablename(a)"))
			})
			It("returns a slice for a table with a single UNIQUE PRIMARY KEY column", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowPrimarySingle...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("tablename_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
				Expect(results[1].ConName).To(Equal("tablename_pkey"))
				Expect(results[1].ConDef).To(Equal("PRIMARY KEY (i)"))
			})
			It("returns a slice for a table with a single UNIQUE column used in a composite PRIMARY KEY on two columns", func() {
				fakeResult := sqlmock.NewRows(header).AddRow(rowOneUnique...).AddRow(rowPrimaryComposite...)
				mock.ExpectQuery("SELECT (.*)").WillReturnRows(fakeResult)
				results := backup.GetPkFkUniqueConstraints(connection, 0)
				Expect(len(results)).To(Equal(2))
				Expect(results[0].ConName).To(Equal("tablename_i_uniq"))
				Expect(results[0].ConDef).To(Equal("UNIQUE (i)"))
				Expect(results[1].ConName).To(Equal("tablename_pkey"))
				Expect(results[1].ConDef).To(Equal("PRIMARY KEY (i, j)"))
			})
		})
	})
})
