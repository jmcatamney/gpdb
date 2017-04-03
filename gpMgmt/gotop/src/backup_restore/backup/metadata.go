package backup

import (
	"fmt"
	"strings"
	"io"
)

func PrintCreateTable(metadataFile io.Writer, tablename string, atts []TableAtts) {
	fmt.Fprintf(metadataFile, "CREATE TABLE %s (\n", tablename)
	lines := make([]string, 0)
	for _, att := range atts {
		line := fmt.Sprintf("\t%s %s", att.AttName, att.AttTypName)
		if att.AttEncoding.Valid {
			line += fmt.Sprintf(" ENCODING(%s)", att.AttEncoding.String)
		}
		lines = append(lines, line)
		// TODO: handle NOT NULL, default values, etc.
	}
	fmt.Fprintln(metadataFile, strings.Join(lines, ",\n"))
	fmt.Fprintln(metadataFile, ");")
}
