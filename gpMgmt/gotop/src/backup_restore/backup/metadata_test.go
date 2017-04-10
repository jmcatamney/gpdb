package backup_test

import (
	"backup_restore/backup"
	"database/sql"
	"regexp"
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
	Describe("PrintCreateTable", func() {
		buffer := gbytes.NewBuffer()
		attsOne := backup.TableAtts{"i", false, false, false, "int", sql.NullString{String: "", Valid:false}}
		attsTwo := backup.TableAtts{"j", false, false, false, "character varying(20)", sql.NullString{String: "", Valid:false}}
		attsEncoded:= backup.TableAtts{"j", false, false, false, "character varying(20)", sql.NullString{String: "compresstype=zlib, blocksize=65536", Valid:true}}
		attsNotNull := backup.TableAtts{"j", true, false, false, "character varying(20)", sql.NullString{String: "", Valid:false}}
		attsEncNotNull := backup.TableAtts{"j", true, false, false, "character varying(20)", sql.NullString{String: "compresstype=zlib, blocksize=65536", Valid:true}}

		Context("Table with one column exists", func() {
			It("Prints a CREATE TABLE block with one line", func() {
				atts := []backup.TableAtts{attsOne}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int
);`)))
			})
		})
		Context("Table with multiple columns exists", func() {
			It("Prints a CREATE TABLE block with one line per attribute", func() {
				atts := []backup.TableAtts{attsOne, attsTwo}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int,
	j character varying(20)
);`)))
			})
		})
		Context("Table with non-NULL attencoding column", func() {
			It("Prints a CREATE TABLE block where one line contains ENCODING", func() {
				atts := []backup.TableAtts{attsOne, attsEncoded}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int,
	j character varying(20) ENCODING(compresstype=zlib, blocksize=65536)
);`)))
			})
		})
		Context("Table with NOT NULL constraint", func() {
			It("Prints a CREATE TABLE block where one line contains NOT NULL", func() {
				atts := []backup.TableAtts{attsOne, attsNotNull}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int,
	j character varying(20) NOT NULL
);`)))
			})
		})
		Context("Table with NOT NULL constraint and non-NULL attencoding column", func() {
			It("Prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				atts := []backup.TableAtts{attsOne, attsEncNotNull}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int,
	j character varying(20) NOT NULL ENCODING(compresstype=zlib, blocksize=65536)
);`)))
			})
		})
	})
})
