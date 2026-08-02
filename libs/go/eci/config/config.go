// Package config fornisce un loader minimo di configurazione da variabili
// d'ambiente (env var con default), usato da ingestion e futuri servizi Go.
package config

import "os"

// EnvOrDefault legge la variabile d'ambiente key; se assente, ritorna
// defaultValue. Una variabile impostata esplicitamente a stringa vuota
// (KEY="") è trattata come valore impostato, non come assenza —
// comportamento standard di os.LookupEnv, non alterato con logica custom
// che confonderebbe "vuoto" con "assente" (SPEC-011 §4).
func EnvOrDefault(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}
