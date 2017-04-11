package backup

import (
	"fmt"
	"io"
	"strings"
)

func PrintCreateTableStatement(metadataFile io.Writer, tablename string, atts []QueryTableAtts, defs []QueryTableDefs) {
	fmt.Fprintf(metadataFile, "\n\nCREATE TABLE %s (\n", tablename)
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
	if len(lines) > 0 {
		fmt.Fprintln(metadataFile, strings.Join(lines, ",\n"))
	}
	fmt.Fprintln(metadataFile, ");")
}

func PrintAlterTableStatements(metadataFile io.Writer, tablename string, constraint []QueryConstraint) {
	constraints := HandleConstraints(tablename, constraint)
	for _, cons := range constraints {
		fmt.Fprintln(metadataFile, cons)
	}
}

func HandleConstraints(tablename string, constraint []QueryConstraint) []string {
	alterStr := fmt.Sprintf("\n\nALTER TABLE ONLY %s ADD CONSTRAINT", tablename)
	constraints := make([]string, 0)
	for _, con := range constraint {
		conStr := fmt.Sprintf("%s %s %s;", alterStr, con.ConName, con.ConDef)
		constraints = append(constraints, conStr)
	}
	return constraints
}
