// Package migrate — SPEC-004 §2. Runner Go che applica il DDL Cypher (D3,
// contracts/cypher/schema.cypher) in modo idempotente contro Neo4j.
package migrate

import "strings"

// ParseStatements spezza il sorgente Cypher in singoli statement sul
// delimitatore top-level ';' (scelto in SPEC-004 §2 come separatore più
// robusto della sola riga vuota), rimuovendo i commenti '//' e le righe
// vuote. Un ';' o '//' dentro una stringa tra apici (', ", `) non è trattato
// come delimitatore/marcatore di commento. Statement vuoti (blocchi
// composti solo da commenti o spazi) vengono scartati.
func ParseStatements(source string) []string {
	var statements []string
	var current strings.Builder

	runes := []rune(source)
	n := len(runes)
	var quote rune

	for i := 0; i < n; i++ {
		c := runes[i]

		if quote != 0 {
			current.WriteRune(c)
			if c == quote {
				quote = 0
			}
			continue
		}

		switch {
		case c == '\'' || c == '"' || c == '`':
			quote = c
			current.WriteRune(c)
		case c == '/' && i+1 < n && runes[i+1] == '/':
			for i < n && runes[i] != '\n' {
				i++
			}
			current.WriteRune('\n')
		case c == ';':
			appendStatement(&statements, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	appendStatement(&statements, current.String())

	return statements
}

func appendStatement(statements *[]string, raw string) {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		*statements = append(*statements, trimmed)
	}
}
