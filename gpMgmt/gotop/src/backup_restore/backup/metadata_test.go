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
		attsDropped := backup.QueryTableAtts{2, "j", false, true, true, "character varying(20)", sql.NullString{String: "", Valid: false}}
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
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int
);`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
);`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				atts := []backup.QueryTableAtts{}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
);`)
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
				atts := []backup.QueryTableAtts{attsOne, attsDropped}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int
);`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsTwoEnc}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				atts := []backup.QueryTableAtts{attsOne, attsNotNull}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20) NOT NULL
);`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				defs := []backup.QueryTableDefs{defsOne}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int DEFAULT 42,
	j character varying(20)
);`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				defs := []backup.QueryTableDefs{defsOne, defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
);`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsEncNotNull}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				atts := []backup.QueryTableAtts{attsOne, attsNotNull}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
);`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsTwoEnc}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsEncNotNull}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING(compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING(compresstype=zlib,blocksize=65536,compresslevel=1)
);`)
			})
		})
	})
	Describe("HandlePkFkUniqueConstraints", func() {
		uniqueOne := backup.QueryPkFkUniqueConstraint{"tablename_i_key", "UNIQUE (i)"}
		uniqueTwo := backup.QueryPkFkUniqueConstraint{"tablename_j_key", "UNIQUE (j)"}
		primarySingle := backup.QueryPkFkUniqueConstraint{"tablename_pkey", "PRIMARY KEY (i)"}
		primaryComposite := backup.QueryPkFkUniqueConstraint{"tablename_pkey", "PRIMARY KEY (i, j)"}
		foreignOne := backup.QueryPkFkUniqueConstraint{"tablename_i_fkey", "FOREIGN KEY (i) REFERENCES other_tablename(a)"}
		foreignTwo := backup.QueryPkFkUniqueConstraint{"tablename_j_fkey", "FOREIGN KEY (j) REFERENCES other_tablename(b)"}

		Context("No ALTER TABLE statements", func() {
			It("returns an empty slice", func() {
				cons := []backup.QueryPkFkUniqueConstraint{}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(0))
			})
		})
		Context("ALTER TABLE statements involving different columns", func() {
			It("returns a slice containing one UNIQUE constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{uniqueOne}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
			})
			It("returns a slice containing two UNIQUE constraints", func() {
				cons := []backup.QueryPkFkUniqueConstraint{uniqueOne, uniqueTwo}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);"))
			})
			It("returns a slice containing PRIMARY KEY constraint on one column", func() {
				cons := []backup.QueryPkFkUniqueConstraint{primarySingle}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
			})
			It("returns a slice containing composite PRIMARY KEY constraint on two columns", func() {
				cons := []backup.QueryPkFkUniqueConstraint{primaryComposite}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
			})
			It("returns a slice containing one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{foreignOne}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(1))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing two FOREIGN KEY constraints", func() {
				cons := []backup.QueryPkFkUniqueConstraint{foreignOne, foreignTwo}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{uniqueOne, foreignTwo}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{primarySingle, foreignTwo}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{primaryComposite, foreignTwo}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
		})
		Context("ALTER TABLE statements involving the same column", func() {
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{uniqueOne, foreignOne}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{primarySingle, foreignOne}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				cons := []backup.QueryPkFkUniqueConstraint{primaryComposite, foreignOne}
				constraints := backup.HandlePkFkUniqueConstraints("tablename", cons)
				Expect(len(constraints)).To(Equal(2))
				Expect(constraints[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(constraints[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
		})
	})
})
