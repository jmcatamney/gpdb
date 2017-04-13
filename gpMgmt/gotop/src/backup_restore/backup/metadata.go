package backup

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

func PrintCreateTableStatement(metadataFile io.Writer, tablename string, atts []QueryTableAtts, defs []QueryTableDefs, distPolicy string, aocoDef string) {
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
				line += fmt.Sprintf(" ENCODING (%s)", att.AttEncoding.String)
			}
			lines = append(lines, line)
		}
	}
	if len(lines) > 0 {
		fmt.Fprintln(metadataFile, strings.Join(lines, ",\n"))
	}
	fmt.Fprintf(metadataFile, ") ")
	if aocoDef != "" {
		fmt.Fprintf(metadataFile, "WITH %s ", aocoDef)
	}
	fmt.Fprintf(metadataFile, "%s;\n", distPolicy)
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
