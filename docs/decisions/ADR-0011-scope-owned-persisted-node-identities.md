# ADR-0011 — Identità persistite dei nodi sotto namespace di ownership

Stato: accettata · Data: 2026-08-29

## Contesto

SPEC-013 definisce l'identità prodotta dal parser come SHA-256 di
`file_path:qualified_name`. Quell'identità è deterministica nel bounded context
del repository, ma non è globalmente univoca: tenant o repository diversi
possono contenere lo stesso path e simbolo. PostgreSQL usa `code_node.id` come
chiave primaria; i sink usano lo stesso valore negli upsert. Limitarsi ad
aggiornare `provenance.tenant_id/repo/acl_group` consente quindi all'ultimo
writer di rietichettare e sostituire il nodo di un altro scope.

Sono state considerate tre alternative:

1. rendere composite tutte le chiavi PostgreSQL/Neo4j e ogni riferimento con
   `(tenant_id, repo, parser_id)`: preserva il parser ID esterno, ma richiede un
   breaking change coordinato di D2, FK, proto e join in tutti gli store;
2. includere tenant/repo direttamente in `parse_file`: confonde parsing puro e
   autenticazione, modifica SPEC-013 e rende il parser dipendente dal caller;
3. derivare al confine di persistenza un ID fisico SHA-256 versionato da
   ownership + parser ID, propagandolo in modo atomico a nodi, relazioni,
   chunk e outbox.

## Decisione

Si adotta l'alternativa 3. `parse_file` continua a produrre il proprio ID
repository-local secondo SPEC-013. `persist_parsed_file` deriva invece:

```text
SHA-256("eci-node-id-v2" || len(tenant_id) || tenant_id
                         || len(repo)      || repo
                         || len(parser_id) || parser_id)
```

Le lunghezze sono unsigned 64-bit big-endian. La stessa funzione viene usata
per `code_node.id`, endpoint `code_relation`, `code_chunk.entity_id` e payload
outbox. Tenant e repository sono dimensioni immutabili di ownership. Il gruppo
ACL non entra nell'identità: un cambio ACL deve aggiornare/revocare l'oggetto
esistente, non lasciare una copia sotto il vecchio gruppo.

Il formato D2 non cambia: `id` resta una stringa opaca e deterministica. Non si
accetta un ID fisico dal caller e non si usano prompt, testo utente o output
LLM. Il namespace deriva soltanto da `IngestionScope` validato.

## Conseguenze

- Lo stesso simbolo nello stesso tenant/repository mantiene un ID stabile e
  gli upsert restano idempotenti.
- Lo stesso parser ID in tenant o repository diversi produce righe e viste
  materializzate disgiunte; delete/replace non attraversano lo scope.
- Un cambio ACL mantiene l'identità e sovrascrive la label, consentendo la
  revoca senza residui leggibili dal gruppo precedente.
- Gli ID persistiti prodotti prima di questa decisione diventano legacy
  unlabeled/non-namespaced. Restano invisibili sotto i filtri positivi T6.3 e
  richiedono una migrazione amministrativa esplicita per essere riattivati.
- Le API continuano a trattare `node_id` come opaco; non esiste parsing o
  reversibilità dell'ID.

## Compatibilità e migrazione

La forma sintattica resta SHA-256 esadecimale di 64 caratteri, quindi schemi,
proto e sink non richiedono breaking change. La semantica dell'ID persistito è
versionata dal prefisso. Un rollout sicuro aggiorna prima producer e consumer
compatibili con label T6.3; i record nuovi ricevono ID v2. Rollback del producer
non rende visibili record altrui perché i filtri positivi restano obbligatori,
ma può creare record legacy non interrogabili e non è una strategia di
migrazione.

## Impatto sicurezza

La decisione rimuove il canale last-writer-wins cross-tenant/cross-repository.
La funzione è deterministica, length-delimited e non accetta fallback. ACL è
intenzionalmente escluso dall'hash per preservare la revocabilità. I test
persistono lo stesso output parser sotto due tenant e richiedono identità,
relazioni e provenance disgiunte.

## Evidenza di attuazione

T6.3 applica `scoped_node_id` prima di ogni write canonica e aggiunge una
regressione PostgreSQL reale. I sink ricevono soltanto gli ID già namespaced
negli eventi outbox; nessun sink ricostruisce autonomamente l'identità.
