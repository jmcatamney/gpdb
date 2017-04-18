package backup

import (
	"backup_restore/utils"
	"database/sql"
	"fmt"
	"io"
	"sort"
	"strings"
)

type ColumnDefinition struct {
	Num       int
	Name      string
	NotNull   bool
	HasDef    bool
	IsDropped bool
	TypName   string
	Encoding  sql.NullString
	DefVal    string
}

type TableDefinition struct {
	DistPolicy  string
	PartDef     string
	StorageOpts string
}

func PrintCreateTableStatement(metadataFile io.Writer, tablename string, columnDefs []ColumnDefinition, tableDef TableDefinition) {
	fmt.Fprintf(metadataFile, "\n\nCREATE TABLE %s (\n", tablename)
	lines := make([]string, 0)
	for _, col := range columnDefs {
		if !col.IsDropped {
			line := fmt.Sprintf("\t%s %s", col.Name, col.TypName)
			if col.HasDef {
				line += fmt.Sprintf(" DEFAULT %s", col.DefVal)
			}
			if col.NotNull {
				line += " NOT NULL"
			}
			if col.Encoding.Valid {
				line += fmt.Sprintf(" ENCODING (%s)", col.Encoding.String)
			}
			lines = append(lines, line)
		}
	}
	if len(lines) > 0 {
		fmt.Fprintln(metadataFile, strings.Join(lines, ",\n"))
	}
	fmt.Fprintf(metadataFile, ") ")
	if tableDef.StorageOpts != "" {
		fmt.Fprintf(metadataFile, "WITH %s ", tableDef.StorageOpts)
	}
	fmt.Fprintf(metadataFile, "%s", tableDef.DistPolicy)
	fmt.Fprintf(metadataFile, "%s;\n", tableDef.PartDef)
}

func ConsolidateColumnInfo(atts []QueryTableAtts, defs []QueryTableDefs) []ColumnDefinition {
	if len(atts) != len(defs) {
		utils.Abort("Attributes array and defaults array must have the same length (attributes length %d, defaults length %d", len(atts), len(defs))
	}
	colDefs := make([]ColumnDefinition, 0)
	// The queries to get attributes and defaults ORDER BY oid and then attribute number, so we can assume the arrays are in the same order without sorting
	for i := range atts {
		colDef := ColumnDefinition{
			Num:       atts[i].AttNum,
			Name:      atts[i].AttName,
			NotNull:   atts[i].AttNotNull,
			HasDef:    atts[i].AttHasDef,
			IsDropped: atts[i].AttIsDropped,
			TypName:   atts[i].AttTypName,
			Encoding:  atts[i].AttEncoding,
			DefVal:    defs[i].DefVal,
		}
		colDefs = append(colDefs, colDef)
	}
	return colDefs
}

func PrintConstraintStatements(metadataFile io.Writer, cons []string, fkCons []string) {
	sort.Strings(cons)
	sort.Strings(fkCons)
	for _, con := range cons {
		fmt.Fprintln(metadataFile, con)
	}
	for _, con := range fkCons {
		fmt.Fprintln(metadataFile, con)
	}
}

func ProcessConstraints(tablename string, constraints []QueryConstraint) ([]string, []string) {
	alterStr := fmt.Sprintf("\n\nALTER TABLE ONLY %s ADD CONSTRAINT", tablename)
	cons := make([]string, 0)
	fkCons := make([]string, 0)
	for _, constraint := range constraints {
		conStr := fmt.Sprintf("%s %s %s;", alterStr, constraint.ConName, constraint.ConDef)
		if constraint.ConType == "f" {
			fkCons = append(fkCons, conStr)
		} else {
			cons = append(cons, conStr)
		}
	}
	return cons, fkCons
}
