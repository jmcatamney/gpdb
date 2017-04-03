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
		attsOne := backup.TableAtts{"i", false, false, "int", sql.NullString{String: "", Valid:false}}
		attsTwo := backup.TableAtts{"j", false, false, "character varying(20)", sql.NullString{String: "", Valid:false}}
		attsThree := backup.TableAtts{"k", false, false, "smallint", sql.NullString{String: "", Valid:false}}
		attsEncoded:= backup.TableAtts{"j", false, false, "character varying(20)", sql.NullString{String: "compresstype=zlib, blocksize=65536", Valid:true}}

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
				atts := []backup.TableAtts{attsOne, attsTwo, attsThree}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int,
	j character varying(20),
	k smallint
);`)))
			})
		})
		Context("Table with non-NULL attencoding column", func() {
			It("Prints a CREATE TABLE block where one line contains ENCODING", func() {
				atts := []backup.TableAtts{attsOne, attsEncoded, attsThree}
				backup.PrintCreateTable(buffer, "foo", atts)
				Expect(buffer).Should(gbytes.Say(regexp.QuoteMeta(`CREATE TABLE foo (
	i int,
	j character varying(20) ENCODING(compresstype=zlib, blocksize=65536),
	k smallint
);`)))
			})
		})
	})
})
