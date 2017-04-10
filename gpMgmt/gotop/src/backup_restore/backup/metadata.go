package backup

import (
	"fmt"
	"io"
	"strings"
)

func PrintCreateTable(metadataFile io.Writer, tablename string, atts []QueryTableAtts, defs []QueryTableDefs) {
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
