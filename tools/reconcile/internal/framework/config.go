package framework

import (
	"flag"
	"fmt"
)

// Config — parametri di un'esecuzione della CLI reconcile (SPEC-037 §2:
// `reconcile --target=<nome>`).
type Config struct {
	DSN    string
	Target string
}

// ParseConfig legge `args` (tipicamente os.Args[1:]) come flag CLI:
// `--dsn` e `--target`, entrambi obbligatori (fail-fast esplicito, stesso
// principio di tools/gc-postgres SPEC-028 §2).
func ParseConfig(args []string) (Config, error) {
	fs := flag.NewFlagSet("reconcile", flag.ContinueOnError)
	dsn := fs.String("dsn", "", "Postgres DSN (obbligatorio)")
	target := fs.String("target", "", "nome del target da riconciliare (obbligatorio)")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}

	if *dsn == "" {
		return Config{}, fmt.Errorf("--dsn è obbligatorio")
	}
	if *target == "" {
		return Config{}, fmt.Errorf("--target è obbligatorio")
	}

	return Config{DSN: *dsn, Target: *target}, nil
}
