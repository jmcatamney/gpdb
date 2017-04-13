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

		distRandom := "DISTRIBUTED RANDOMLY"
		distSingle := "DISTRIBUTED BY (i)"
		distComposite := "DISTRIBUTED BY (i, j)"

		heapDef := ""
		aoDef := "(appendonly=true)"
		coDef := "(appendonly=true, orientation=column)"

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE block with one line", func() {
				atts := []backup.QueryTableAtts{attsOne}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				atts := []backup.QueryTableAtts{}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
				atts := []backup.QueryTableAtts{attsOne, attsDropped}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsTwoEnc}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				atts := []backup.QueryTableAtts{attsOne, attsNotNull}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				defs := []backup.QueryTableDefs{defsOne}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				defs := []backup.QueryTableDefs{defsOne, defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsEncNotNull}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				atts := []backup.QueryTableAtts{attsOne, attsNotNull}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsTwoEnc}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				atts := []backup.QueryTableAtts{attsOneEnc, attsEncNotNull}
				defs := []backup.QueryTableDefs{defsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defs, distRandom, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table qualities (distribution keys and table type)", func() {
			It("has a single-column distribution key", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distSingle, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distComposite, heapDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, aoDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distSingle, aoDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distComposite, aoDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distRandom, coDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distSingle, coDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				atts := []backup.QueryTableAtts{attsOne, attsTwo}
				backup.PrintCreateTableStatement(buffer, "tablename", atts, defsEmpty, distComposite, coDef)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
		})
	})
	Describe("ProcessConstraints", func() {
		uniqueOne := backup.QueryConstraint{"tablename_i_key", "u", "UNIQUE (i)"}
		uniqueTwo := backup.QueryConstraint{"tablename_j_key", "u", "UNIQUE (j)"}
		primarySingle := backup.QueryConstraint{"tablename_pkey", "p", "PRIMARY KEY (i)"}
		primaryComposite := backup.QueryConstraint{"tablename_pkey", "p", "PRIMARY KEY (i, j)"}
		foreignOne := backup.QueryConstraint{"tablename_i_fkey", "f", "FOREIGN KEY (i) REFERENCES other_tablename(a)"}
		foreignTwo := backup.QueryConstraint{"tablename_j_fkey", "f", "FOREIGN KEY (j) REFERENCES other_tablename(b)"}

		Context("No ALTER TABLE statements", func() {
			It("returns an empty slice", func() {
				constraints := []backup.QueryConstraint{}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(0))
			})
		})
		Context("ALTER TABLE statements involving different columns", func() {
			It("returns a slice containing one UNIQUE constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
			})
			It("returns a slice containing two UNIQUE constraints", func() {
				constraints := []backup.QueryConstraint{uniqueOne, uniqueTwo}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(2))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(cons[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_key UNIQUE (j);"))
			})
			It("returns a slice containing PRIMARY KEY constraint on one column", func() {
				constraints := []backup.QueryConstraint{primarySingle}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
			})
			It("returns a slice containing composite PRIMARY KEY constraint on two columns", func() {
				constraints := []backup.QueryConstraint{primaryComposite}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(0))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
			})
			It("returns a slice containing one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{foreignOne}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(1))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing two FOREIGN KEY constraints", func() {
				constraints := []backup.QueryConstraint{foreignOne, foreignTwo}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(0))
				Expect(len(fkCons)).To(Equal(2))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
				Expect(fkCons[1]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne, foreignTwo}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primarySingle, foreignTwo}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primaryComposite, foreignTwo}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_j_fkey FOREIGN KEY (j) REFERENCES other_tablename(b);"))
			})
		})
		Context("ALTER TABLE statements involving the same column", func() {
			It("returns a slice containing one UNIQUE constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{uniqueOne, foreignOne}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_key UNIQUE (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing one PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primarySingle, foreignOne}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
			It("returns a slice containing a two-column composite PRIMARY KEY constraint and one FOREIGN KEY constraint", func() {
				constraints := []backup.QueryConstraint{primaryComposite, foreignOne}
				cons, fkCons := backup.ProcessConstraints("tablename", constraints)
				Expect(len(cons)).To(Equal(1))
				Expect(len(fkCons)).To(Equal(1))
				Expect(cons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_pkey PRIMARY KEY (i, j);"))
				Expect(fkCons[0]).To(Equal("\n\nALTER TABLE ONLY tablename ADD CONSTRAINT tablename_i_fkey FOREIGN KEY (i) REFERENCES other_tablename(a);"))
			})
		})
	})
})
