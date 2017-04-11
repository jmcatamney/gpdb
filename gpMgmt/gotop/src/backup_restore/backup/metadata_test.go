package backup_test

import (
	"backup_restore/backup"
	"backup_restore/testutils"
	"database/sql"
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

func TestMetadata(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "metadata.go unit tests")
}

var _ = Describe("backup/metadata tests", func() {
	Describe("PrintCreateTableStatement", func() {
		buffer := gbytes.NewBuffer()
		attsOne := backup.QueryTableAtts{1, "i", false, true, false, "int", sql.NullString{String: "", Valid: false}}
		attsTwo := backup.QueryTableAtts{2, "j", false, true, false, "character varying(20)", sql.NullString{String: "", Valid: false}}
		attsOneEnc := backup.QueryTableAtts{1, "i", false, false, false, "int", sql.NullString{String: "compresstype=none,blocksize=32768,compresslevel=0", Valid: true}}
		attsTwoEnc := backup.QueryTableAtts{2, "j", false, true, false, "character varying(20)", sql.NullString{String: "compresstype=zlib,blocksize=65536,compresslevel=1", Valid: true}}
		attsNotNull := backup.QueryTableAtts{2, "j", true, true, false, "character varying(20)", sql.NullString{String: "", Valid: false}}
		attsEncNotNull := backup.QueryTableAtts{2, "j", true, true, false, "character varying(20)", sql.NullString{String: "compresstype=zlib,blocksize=65536,compresslevel=1", Valid: true}}

		defsOne := backup.QueryTableDefs{1, "42"}
		defsTwo := backup.QueryTableDefs{2, "'bar'::text"}
		defsEmpty := []backup.QueryTableDefs{}

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE block with one line", func() {
				atts := []backup.QueryTableAtts{attsOne}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int
);`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int,
	j character varying(20)
);`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsTwoEnc}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				atts := []backup.QueryTableAtts{attsOne, attsNotNull}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int,
	j character varying(20) NOT NULL
);`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				defs := []backup.QueryTableDefs{defsOne}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int DEFAULT 42,
	j character varying(20)
);`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				defs := []backup.QueryTableDefs{defsOne, defsTwo}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
);`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsEncNotNull}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				atts := []backup.QueryTableAtts{attsOne, attsNotNull}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
);`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsTwoEnc}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsEncNotNull}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "foo", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE foo (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
		})
	})
})
