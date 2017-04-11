package backup

import (
	"fmt"
	"io"
	"strings"
)

func PrintCreateTableStatement(metadataFile io.Writer, tablename string, atts []QueryTableAtts, defs []QueryTableDefs) {
	fmt.Fprintf(metadataFile, "CREATE TABLE %s (\n", tablename)
	lines := make([]string, 0)
	for _, att := range atts {
		if !att.AttIsDropped {
			line := fmt.Sprintf("\t%s %s", att.AttName, att.AttTypName)
			if att.AttHasDef {
				for _, def := range defs {
					if def.AdNum == att.AttNum {
						line += fmt.Sprintf(" DEFAULT %s", def.DefVal)
					}
				}
			}
			if att.AttNotNull {
				line += " NOT NULL"
			}
			if att.AttEncoding.Valid {
				line += fmt.Sprintf(" ENCODING(%s)", att.AttEncoding.String)
			}
			lines = append(lines, line)
		}
	}
	fmt.Fprintln(metadataFile, strings.Join(lines, ",\n"))
	fmt.Fprintln(metadataFile, ");")
}

func PrintAlterTableStatements(metadataFile io.Writer, tablename string, primaryunique []QueryPrimaryUniqueConstraint) {
	constraints := HandlePrimaryUniqueConstraints(tablename, primaryunique)
	for _, cons := range constraints {
		fmt.Fprintln(metadataFile, cons)
	}
}

func HandlePrimaryUniqueConstraints(tablename string, primaryunique []QueryPrimaryUniqueConstraint) []string {
	alterStr := fmt.Sprintf("ALTER TABLE ONLY %s ADD CONSTRAINT", tablename)
	constraints := make([]string, 0)
	primaries := make([]string, 0)
	for _, con := range primaryunique {
		if con.IsUnique && !con.IsPrimary{
			uniqueStr := fmt.Sprintf("%s %s_%s_key UNIQUE (%s)", alterStr, tablename, con.AttName, con.AttName)
			constraints = append(constraints, uniqueStr)
		}
		if con.IsPrimary {
			primaries = append(primaries, con.AttName)
		}
	}
	if len(primaries) > 0 {
		primaryStr := fmt.Sprintf("%s %s_pkey PRIMARY KEY (%s)", alterStr, tablename, strings.Join(primaries, ", "))
		constraints = append(constraints, primaryStr)
	}
	return constraints
}
