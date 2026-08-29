# ADR-0012 — Chiavi cache derivate dallo scope ACL autenticato

Stato: accepted
Data: 2026-08-29
Decisione collegata: T6.4 / SPEC-058

## Contesto

`CacheKey.acl_scope` esiste dal T2.3, ma SPEC-022 lo definiva esplicitamente
come plumbing fidato dal chiamante e ammetteva il placeholder `default`. In
Fase 6 questo non è sufficiente: un client compromesso potrebbe scegliere lo
scope di un'altra identità e interrogare la relativa entry. Inoltre la chiave
Redis storica concatena campi arbitrari con `:`, quindi valori contenenti il
separatore possono produrre collisioni tra tuple diverse.

L'ADD Modulo 3 §2.6.3 richiede la partizione
`ast_hash + logic_fingerprint + acl_scope` e vieta il riuso cross-ACL. T6.1 e
T6.2 hanno già stabilito che l'unica origine attendibile del
`SecurityContext` sono metadata JWT validati e propagati dagli interceptor.
Redis è una cache ricostruibile, non una fonte di verità.

## Opzioni considerate

1. **Continuare a fidarsi di `CacheKey.acl_scope`.** Compatibile con i client
   storici, ma lascia al chiamante il controllo del confine di autorizzazione.
   Scartata perché contraddice il fail-closed di Fase 6.
2. **Ignorare il campo e sostituirlo silenziosamente con lo scope server-side.**
   Impedisce il leakage, ma nasconde client obsoleti o compromessi e rende
   impossibile distinguere un errore di propagazione da un miss.
3. **Derivare lo scope server-side e richiedere uguaglianza esatta.** Il server
   calcola un fingerprint canonico dal contesto autenticato, rifiuta campo
   assente/diverso prima di Redis e usa soltanto il valore derivato. Le tuple
   sono poi codificate con length-prefix e hashate. È l'opzione scelta.

## Decisione

Il fingerprint `acl_scope` è SHA-256 esadecimale del seguente stream binario:

```text
"eci-acl-scope-v1"
u32be(len(tenant_id)) || utf8(tenant_id)
u32be(repo_count) || repeated(u32be(len(repo)) || utf8(repo))
u32be(group_count) || repeated(u32be(len(group)) || utf8(group))
```

Repository e gruppi sono validati, deduplicati e ordinati prima del calcolo.
`user_id` non partecipa: utenti con identici permessi possono condividere in
sicurezza lo stesso risultato; `trace_id`, prompt e request body non sono mai
input. Tenant, repository o gruppi vuoti sono invalidi.

`SemanticCache.Get/Put` deriva questo valore da `secctx`/`accessscope`, richiede
che `CacheKey.acl_scope` coincida in constant time e rifiuta mismatch o assenza
con `PermissionDenied` prima di qualunque I/O Redis. La chiave fisica diventa:

```text
scache:v2:<sha256(length-prefixed entity_id, ast_hash,
                   logic_fingerprint, derived_acl_scope)>
```

Il proto v1 non cambia forma né tag; cambia la semantica del campo già
riservato all'enforcement di Fase 6. Client con `default` devono calcolare e
propagare il fingerprint canonico.

## Conseguenze

- Un client non può sondare o scrivere una partizione ACL diversa dalla
  propria; metadata assenti o malformati falliscono chiusi.
- Ordinamenti o duplicati equivalenti producono lo stesso fingerprint in Go e
  Python; valori diversi non collidono per ambiguità di separatore.
- Le entry `scache:*` storiche non sono lette da `scache:v2:*`. È una
  invalidazione intenzionale e reversibile tramite ricomputazione; nessuna
  migrazione dati è necessaria perché Redis non è source of truth.
- Un rollback al codice precedente torna a leggere solo le vecchie entry e
  perde i nuovi hit, senza corrompere dati canonici. Non è consentito usare il
  rollback per reintrodurre il placeholder in produzione.

## Impatto sicurezza e compatibilità

La modifica è fail-closed e riduce la fiducia nel client. Non usa testo utente,
LLM, expected data o filtri post-hoc. La compatibilità wire resta invariata;
la compatibilità comportamentale è deliberatamente interrotta per i client di
Fase 2 che inviano scope vuoto/`default`, come richiesto dal passaggio a Fase 6.
Test cross-language fissano l'algoritmo e test gRPC+Redis provano il diniego
prima dello storage.

