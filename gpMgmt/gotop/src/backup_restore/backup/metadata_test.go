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
		rowOne := backup.ColumnDefinition{1, "i", false, false, false, "int", sql.NullString{String: "", Valid: false}, ""}
		rowTwo := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", sql.NullString{String: "", Valid: false}, ""}
		rowDropped := backup.ColumnDefinition{2, "j", false, false, true, "character varying(20)", sql.NullString{String: "", Valid: false}, ""}
		rowOneEnc := backup.ColumnDefinition{1, "i", false, false, false, "int", sql.NullString{String: "compresstype=none,blocksize=32768,compresslevel=0", Valid: true}, ""}
		rowTwoEnc := backup.ColumnDefinition{2, "j", false, false, false, "character varying(20)", sql.NullString{String: "compresstype=zlib,blocksize=65536,compresslevel=1", Valid: true}, ""}
		rowNotNull := backup.ColumnDefinition{2, "j", true, false, false, "character varying(20)", sql.NullString{String: "", Valid: false}, ""}
		rowEncNotNull := backup.ColumnDefinition{2, "j", true, false, false, "character varying(20)", sql.NullString{String: "compresstype=zlib,blocksize=65536,compresslevel=1", Valid: true}, ""}
		rowOneDef := backup.ColumnDefinition{1, "i", false, true, false, "int", sql.NullString{String: "", Valid: false}, "42"}
		rowTwoDef := backup.ColumnDefinition{2, "j", false, true, false, "character varying(20)", sql.NullString{String: "", Valid: false}, "'bar'::text"}
		rowTwoEncDef := backup.ColumnDefinition{2, "j", false, true, false, "character varying(20)", sql.NullString{String: "compresstype=zlib,blocksize=65536,compresslevel=1", Valid: true}, "'bar'::text"}
		rowNotNullDef := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", sql.NullString{String: "", Valid: false}, "'bar'::text"}
		rowEncNotNullDef := backup.ColumnDefinition{2, "j", true, true, false, "character varying(20)", sql.NullString{String: "compresstype=zlib,blocksize=65536,compresslevel=1", Valid: true}, "'bar'::text"}

		distRandom := "DISTRIBUTED RANDOMLY"
		distSingle := "DISTRIBUTED BY (i)"
		distComposite := "DISTRIBUTED BY (i, j)"

		emptyPartDef := ""

		heapOpts := ""
		aoOpts := "appendonly=true"
		coOpts := "appendonly=true, orientation=column"
		heapFillOpts := "fillfactor=42"
		coManyOpts := "appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1"

		Context("No special table attributes", func() {
			It("prints a CREATE TABLE block with one line", func() {
				col := []backup.ColumnDefinition{rowOne}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with one line per attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block with no attributes", func() {
				col := []backup.ColumnDefinition{}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block without a dropped attribute", func() {
				col := []backup.ColumnDefinition{rowOne, rowDropped}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("One special table attribute", func() {
			It("prints a CREATE TABLE block where one line has the given ENCODING and the other has the default ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEnc, rowTwoEnc}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNull}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20) NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwo}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int DEFAULT 42,
	j character varying(20)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where both lines contain DEFAULT", func() {
				col := []backup.ColumnDefinition{rowOneDef, rowTwoDef}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int DEFAULT 42,
	j character varying(20) DEFAULT 'bar'::text
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Multiple special table attributes on one column", func() {
			It("prints a CREATE TABLE block where one line contains both NOT NULL and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEnc, rowEncNotNull}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and NOT NULL", func() {
				col := []backup.ColumnDefinition{rowOne, rowNotNullDef}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20) DEFAULT 'bar'::text NOT NULL
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains both DEFAULT and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEnc, rowTwoEncDef}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
			It("prints a CREATE TABLE block where one line contains all three of DEFAULT, NOT NULL, and ENCODING", func() {
				col := []backup.ColumnDefinition{rowOneEnc, rowEncNotNullDef}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int ENCODING (compresstype=none,blocksize=32768,compresslevel=0),
	j character varying(20) DEFAULT 'bar'::text NOT NULL ENCODING (compresstype=zlib,blocksize=65536,compresslevel=1)
) DISTRIBUTED RANDOMLY;`)
			})
		})
		Context("Table qualities (distribution keys and storage options)", func() {
			It("has a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distSingle, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) DISTRIBUTED BY (i);`)
			})
			It("has a multiple-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distComposite, emptyPartDef, heapOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distRandom, emptyPartDef, aoOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized table with a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distSingle, emptyPartDef, aoOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized table with a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distComposite, emptyPartDef, aoOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distRandom, emptyPartDef, coOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distSingle, emptyPartDef, coOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distComposite, emptyPartDef, coOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (i, j);`)
			})
			It("is a heap table with a fill factor", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distRandom, emptyPartDef, heapFillOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED RANDOMLY;`)
			})
			It("is a heap table with a fill factor and a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distSingle, emptyPartDef, heapFillOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i);`)
			})
			It("is a heap table with a fill factor and a multiple-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distComposite, emptyPartDef, heapFillOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (fillfactor=42) DISTRIBUTED BY (i, j);`)
			})
			It("is an append-optimized column-oriented table with complex storage options", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distRandom, emptyPartDef, coManyOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED RANDOMLY;`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a single-column distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distSingle, emptyPartDef, coManyOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i);`)
			})
			It("is an append-optimized column-oriented table with complex storage options and a two-column composite distribution key", func() {
				col := []backup.ColumnDefinition{rowOne, rowTwo}
				table := backup.TableDefinition{distComposite, emptyPartDef, coManyOpts}
				backup.PrintCreateTableStatement(buffer, "tablename", col, table)
				testutils.ExpectRegexp(buffer, `CREATE TABLE tablename (
	i int,
	j character varying(20)
) WITH (appendonly=true, orientation=column, fillfactor=42, compresstype=zlib, blocksize=32768, compresslevel=1) DISTRIBUTED BY (i, j);`)
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
	Describe("ConsolidateColumnInfo", func() {
		attsOne := backup.QueryTableAtts{1, "i", false, false, false, "int", sql.NullString{String: "", Valid: false}}
		attsTwo := backup.QueryTableAtts{2, "j", false, false, false, "int", sql.NullString{String: "", Valid: false}}
		attsThree := backup.QueryTableAtts{3, "k", false, false, false, "int", sql.NullString{String: "", Valid: false}}
		attsOneDef := backup.QueryTableAtts{1, "i", false, true, false, "int", sql.NullString{String: "", Valid: false}}
		attsTwoDef := backup.QueryTableAtts{2, "j", false, true, false, "int", sql.NullString{String: "", Valid: false}}
		attsThreeDef := backup.QueryTableAtts{3, "k", false, true, false, "int", sql.NullString{String: "", Valid: false}}

		defsOne := backup.QueryTableDefs{1, "1"}
		defsTwo := backup.QueryTableDefs{2, "2"}
		defsThree := backup.QueryTableDefs{3, "3"}
		It("has no DEFAULT columns", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwo, attsThree}
			defs := []backup.QueryTableDefs{}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal(""))
			Expect(info[1].DefVal).To(Equal(""))
			Expect(info[2].DefVal).To(Equal(""))
		})
		It("has one DEFAULT column (i)", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwo, attsThree}
			defs := []backup.QueryTableDefs{defsOne}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal("1"))
			Expect(info[1].DefVal).To(Equal(""))
			Expect(info[2].DefVal).To(Equal(""))
		})
		It("has one DEFAULT column (j)", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwoDef, attsThree}
			defs := []backup.QueryTableDefs{defsTwo}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal(""))
			Expect(info[1].DefVal).To(Equal("2"))
			Expect(info[2].DefVal).To(Equal(""))
		})
		It("has one DEFAULT column (k)", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwo, attsThreeDef}
			defs := []backup.QueryTableDefs{defsThree}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal(""))
			Expect(info[1].DefVal).To(Equal(""))
			Expect(info[2].DefVal).To(Equal("3"))
		})
		It("has two DEFAULT columns (i and j)", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwoDef, attsThree}
			defs := []backup.QueryTableDefs{defsOne, defsTwo}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal("1"))
			Expect(info[1].DefVal).To(Equal("2"))
			Expect(info[2].DefVal).To(Equal(""))
		})
		It("has two DEFAULT columns (j and k)", func() {
			atts := []backup.QueryTableAtts{attsOne, attsTwoDef, attsThreeDef}
			defs := []backup.QueryTableDefs{defsTwo, defsThree}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal(""))
			Expect(info[1].DefVal).To(Equal("2"))
			Expect(info[2].DefVal).To(Equal("3"))
		})
		It("has two DEFAULT columns (i and k)", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwo, attsThreeDef}
			defs := []backup.QueryTableDefs{defsOne, defsThree}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal("1"))
			Expect(info[1].DefVal).To(Equal(""))
			Expect(info[2].DefVal).To(Equal("3"))
		})
		It("has all DEFAULT columns", func() {
			atts := []backup.QueryTableAtts{attsOneDef, attsTwoDef, attsThreeDef}
			defs := []backup.QueryTableDefs{defsOne, defsTwo, defsThree}
			info := backup.ConsolidateColumnInfo(atts, defs)
			Expect(info[0].DefVal).To(Equal("1"))
			Expect(info[1].DefVal).To(Equal("2"))
			Expect(info[2].DefVal).To(Equal("3"))
		})
	})
})
