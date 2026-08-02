/// Legge la variabile d'ambiente `key`; se assente, ritorna `default`.
/// Stesso pattern già usato in `tools/migrate-neo4j` (Go, `envOrDefault`),
/// qui in Rust per uso da ingestion e futuri servizi Rust.
///
/// Una variabile impostata esplicitamente a stringa vuota (`KEY=""`) è
/// trattata come **valore impostato** (stringa vuota), non come "assente"
/// — comportamento standard di `std::env::var`, non alterato con logica
/// custom che confonderebbe "vuoto" con "assente" (SPEC-010 §4).
pub fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(test)]
mod tests {
    use super::env_or_default;

    // Ogni test usa una chiave env univoca: env::set_var muta lo stato di
    // processo condiviso e cargo test esegue i test in thread paralleli di
    // default — chiavi distinte evitano race condition tra test.

    #[test]
    fn scenario4_returns_default_when_unset() {
        let key = "ECI_TEST_CONFIG_UNSET_G4A";
        std::env::remove_var(key);
        assert_eq!(env_or_default(key, "./sample-repo"), "./sample-repo");
    }

    #[test]
    fn scenario4_returns_override_when_set() {
        let key = "ECI_TEST_CONFIG_OVERRIDE_G4B";
        std::env::set_var(key, "/custom/path");
        assert_eq!(env_or_default(key, "./sample-repo"), "/custom/path");
        std::env::remove_var(key);
    }

    #[test]
    fn edge_case_explicit_empty_string_is_not_treated_as_absent() {
        let key = "ECI_TEST_CONFIG_EMPTY_G4C";
        std::env::set_var(key, "");
        assert_eq!(env_or_default(key, "./sample-repo"), "");
        std::env::remove_var(key);
    }
}
