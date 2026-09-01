//go:build integration

// SPEC-008 — smoke test end-to-end: un INSERT reale nella tabella outbox
// produce, attraverso Debezium + l'EventRouter SMT, un messaggio Kafka
// leggibile sul topic corretto. Harness testcontainers a 3 container
// (postgres, kafka, kafka-connect) su una rete di test dedicata,
// indipendente dallo stack compose persistente di SPEC-006/007.
//
// Topic/chiave/valore attesi (SPEC-008 §2), verificati contro il sorgente
// Debezium v3.6.0.Final (stessa versione bundlata in
// l'immagine Quay ufficiale di Debezium bloccata per digest, vedi deviazione in fondo al file per i
// riferimenti):
//   - route.by.field=aggregate_type, route.topic.replacement di default
//     "outbox.event.${routedByValue}" -> topic = outbox.event.<aggregate_type>.
//   - table.field.event.key=aggregate_id -> chiave del messaggio = aggregate_id.
//   - nessun table.fields.additional.placement configurato -> EventRouterDelegate
//     considera onlyHeadersInOutputMessage=true (vacuously true su lista
//     vuota) -> il valore del messaggio è il solo contenuto della colonna
//     payload, non un envelope con before/after/source.
package outbox_cdc_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	postgresAlias        = "outbox-cdc-postgres"
	kafkaAlias           = "outbox-cdc-kafka"
	postgresInternalPort = "5432"
	// user/password/dbname devono coincidere con quelli già scritti (non
	// sostituiti) in deploy/compose/debezium-outbox-connector.json — SPEC-008
	// §2 sostituisce SOLO database.hostname/database.port, tutti gli altri
	// campi restano quelli del file reale.
	postgresUser     = "eci"
	postgresPassword = "eci-dev-only"
	postgresDB       = "eci"
	connectorName    = "eci-outbox-connector"
	registerTimeout  = 60 * time.Second
	runningTimeout   = 60 * time.Second
	pollInterval     = 3 * time.Second
)

func TestOutboxCDCEndToEnd(t *testing.T) {
	ctx := context.Background()
	st := setupStack(t, ctx)

	// Scenario 5: nessun messaggio spurio prima di qualunque INSERT.
	t.Run("Scenario5_NoMessageBeforeAnyInsert", func(t *testing.T) {
		if key, value, ok := tryConsumeOne(t, ctx, st.kafka, "outbox.event.CodeNode", 5*time.Second); ok {
			t.Fatalf("messaggio inatteso su outbox.event.CodeNode prima di qualunque INSERT: key=%q value=%q", key, value)
		}
		if key, value, ok := tryConsumeOne(t, ctx, st.kafka, "outbox.event.CodeRelation", 5*time.Second); ok {
			t.Fatalf("messaggio inatteso su outbox.event.CodeRelation prima di qualunque INSERT: key=%q value=%q", key, value)
		}
	})

	// Scenari 1/2/3: INSERT CodeNode -> messaggio su outbox.event.CodeNode,
	// chiave = aggregate_id, valore = solo il contenuto di payload.
	const codeNodeAggID = "code-node-aggregate-42"
	const codeNodePayload = `{"symbol_id":"pkg.Foo#Bar()","language":"go"}`
	insertOutboxRow(t, ctx, st.db, "11111111-1111-1111-1111-111111111111", "CodeNode", codeNodeAggID, "UPSERT", codeNodePayload)

	t.Run("Scenario1_2_3_CodeNodeMessage", func(t *testing.T) {
		key, value, ok := tryConsumeOne(t, ctx, st.kafka, "outbox.event.CodeNode", 30*time.Second)
		if !ok {
			t.Fatalf("nessun messaggio ricevuto su outbox.event.CodeNode entro 30s dopo l'INSERT")
		}
		t.Logf("raw key=%q raw value=%q", key, value)
		if got := decodeJSONString(t, key); got != codeNodeAggID {
			t.Errorf("chiave del messaggio = %q, want %q (aggregate_id)", got, codeNodeAggID)
		}
		assertJSONEqual(t, value, codeNodePayload)
	})

	// Scenario 4: INSERT CodeRelation -> messaggio sul topic DISTINTO
	// outbox.event.CodeRelation (routing per aggregate_type verificato a
	// discriminare, non solo a funzionare per un caso singolo).
	const codeRelAggID = "code-relation-aggregate-99"
	const codeRelPayload = `{"rel_type":"CALLS","weight":0.75}`
	insertOutboxRow(t, ctx, st.db, "22222222-2222-2222-2222-222222222222", "CodeRelation", codeRelAggID, "UPSERT", codeRelPayload)

	t.Run("Scenario4_CodeRelationDistinctTopic", func(t *testing.T) {
		key, value, ok := tryConsumeOne(t, ctx, st.kafka, "outbox.event.CodeRelation", 30*time.Second)
		if !ok {
			t.Fatalf("nessun messaggio ricevuto su outbox.event.CodeRelation entro 30s dopo l'INSERT")
		}
		t.Logf("raw key=%q raw value=%q", key, value)
		if got := decodeJSONString(t, key); got != codeRelAggID {
			t.Errorf("chiave del messaggio = %q, want %q (aggregate_id)", got, codeRelAggID)
		}
		assertJSONEqual(t, value, codeRelPayload)
	})

	// SPEC-071: il discriminante canonico viaggia in un header separato;
	// topic, key e payload conservano il comportamento EventRouter esistente.
	const deleteChunkID = "33333333-3333-3333-3333-333333333333"
	const deleteChunkPayload = `{"id":"33333333-3333-3333-3333-333333333333","entity_id":"node-42"}`
	deleteSequence := insertOutboxRow(t, ctx, st.db, deleteChunkID, "CodeChunk", deleteChunkID, "DELETE", deleteChunkPayload)

	t.Run("SPEC071_DELETECarriesSingleOperationHeader", func(t *testing.T) {
		headers, key, value, ok := tryConsumeOneWithHeaders(
			t, ctx, st.kafka, "outbox.event.CodeChunk", 30*time.Second,
		)
		if !ok {
			t.Fatal("nessun DELETE CodeChunk ricevuto entro 30s")
		}
		if got := decodeJSONString(t, key); got != deleteChunkID {
			t.Errorf("chiave = %q, want %q", got, deleteChunkID)
		}
		assertJSONEqual(t, value, deleteChunkPayload)
		if strings.Count(headers, "event_type:DELETE") != 1 {
			t.Fatalf("header event_type DELETE assente o duplicato: %q", headers)
		}
		sequenceHeader := fmt.Sprintf("event_sequence:%d", deleteSequence)
		if strings.Count(headers, sequenceHeader) != 1 {
			t.Fatalf("header %s assente o duplicato: %q", sequenceHeader, headers)
		}
		for _, forbidden := range []string{"tenant", "repository", "acl", "path", "payload"} {
			if strings.Contains(strings.ToLower(headers), forbidden) {
				t.Fatalf("header CDC espone campo vietato %q: %q", forbidden, headers)
			}
		}
	})
}

// ============================================================
// Harness: 3 container testcontainers su rete dedicata.
// ============================================================

type stack struct {
	kafka      testcontainers.Container
	db         *sql.DB
	connectURL string
}

func setupStack(t *testing.T, ctx context.Context) *stack {
	t.Helper()

	net, err := tcnetwork.New(ctx)
	if err != nil {
		t.Fatalf("creazione rete di test dedicata: %v", err)
	}
	t.Cleanup(func() {
		if err := net.Remove(ctx); err != nil {
			t.Logf("rimozione rete di test: %v", err)
		}
	})

	postgres := startPostgres(t, ctx, net.Name)
	applyMigration(t, ctx, postgres)
	db := connectPostgres(t, ctx, postgres)

	kafka := startKafka(t, ctx, net.Name)
	kafkaConnect := startKafkaConnect(t, ctx, net.Name)

	connectURL := kafkaConnectURL(t, ctx, kafkaConnect)
	connectorJSON := buildConnectorConfig(t)
	registerConnector(t, ctx, connectURL, connectorJSON)
	waitConnectorRunning(t, ctx, connectURL)

	return &stack{kafka: kafka, db: db, connectURL: connectURL}
}

func startPostgres(t *testing.T, ctx context.Context, networkName string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		Image: "postgres:17",
		Env: map[string]string{
			"POSTGRES_USER":     postgresUser,
			"POSTGRES_PASSWORD": postgresPassword,
			"POSTGRES_DB":       postgresDB,
		},
		// Stessa configurazione wal_level=logical di SPEC-007 §2: senza,
		// Debezium non può fare logical decoding.
		Cmd:            []string{"postgres", "-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4"},
		ExposedPorts:   []string{postgresInternalPort + "/tcp"},
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {postgresAlias}},
		WaitingFor:     wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("avvio container postgres:17: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(ctx); err != nil {
			t.Logf("terminazione container postgres: %v", err)
		}
	})
	return c
}

func hostDSN(t *testing.T, ctx context.Context, postgres testcontainers.Container) string {
	t.Helper()
	host, err := postgres.Host(ctx)
	if err != nil {
		t.Fatalf("postgres Host: %v", err)
	}
	port, err := postgres.MappedPort(ctx, postgresInternalPort+"/tcp")
	if err != nil {
		t.Fatalf("postgres MappedPort: %v", err)
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", postgresUser, postgresPassword, host, port.Port(), postgresDB)
}

// applyMigration applica contracts/sql/migrations/0001_init.up.sql via il
// CLI `migrate` (stesso meccanismo di `task db:migrate`, SPEC-005), PRIMA
// di registrare il connector — SPEC-008 §2/§4: se la tabella outbox non
// esiste ancora, Debezium logga "no changes will be captured" (SPEC-007) e
// il test fallirebbe per sequencing, non per logica.
func applyMigration(t *testing.T, ctx context.Context, postgres testcontainers.Container) {
	t.Helper()
	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v", err)
	}
	dsn := hostDSN(t, ctx, postgres)
	migrationsDir := repoPath(t, "contracts", "sql", "migrations")
	cmd := exec.CommandContext(ctx, "migrate", "-source", "file://"+migrationsDir, "-database", dsn, "up")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("migrate up fallita: %v\noutput:\n%s", err, out)
	}
}

func connectPostgres(t *testing.T, ctx context.Context, postgres testcontainers.Container) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", hostDSN(t, ctx, postgres))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	return db
}

func startKafka(t *testing.T, ctx context.Context, networkName string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		// :latest verificato al momento dell'implementazione di SPEC-007
		// (risolve a Kafka 4.3.1, KRaft-only) — stessa immagine riusata qui.
		Image: "apache/kafka:latest",
		Env: map[string]string{
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_LISTENERS":                                "PLAINTEXT://:9092,CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://" + kafkaAlias + ":9092",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@" + kafkaAlias + ":9093",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		},
		// Nessun listener EXTERNAL/porta host esposta: a differenza dello
		// stack compose di SPEC-007 (dev interattivo), qui nulla sull'host
		// deve raggiungere Kafka direttamente — kafka-connect lo raggiunge
		// via alias di rete interno, e questo test stesso consuma i
		// messaggi via `Exec` dentro il container (kafka-console-consumer.sh),
		// non via un client Kafka dall'host. Deviazione annotata in fondo
		// al file.
		Networks:       []string{networkName},
		NetworkAliases: map[string][]string{networkName: {kafkaAlias}},
		WaitingFor: wait.ForExec([]string{"/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "localhost:9092"}).
			WithStartupTimeout(60 * time.Second).WithPollInterval(2 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("avvio container apache/kafka:latest: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(ctx); err != nil {
			t.Logf("terminazione container kafka: %v", err)
		}
	})
	return c
}

func startKafkaConnect(t *testing.T, ctx context.Context, networkName string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		// debezium/connect:latest non esiste su Docker Hub (deviazione storica SPEC-007 §10
		// deviazione #1) — stessa immagine ufficiale bloccata per digest.
		Image: "quay.io/debezium/connect@sha256:698f0559e667a242f962221079e75917b2b7a3ad4de62661e977628da0e33b45",
		Env: map[string]string{
			"BOOTSTRAP_SERVERS":    kafkaAlias + ":9092",
			"GROUP_ID":             "eci-connect-test",
			"CONFIG_STORAGE_TOPIC": "eci_connect_configs",
			"OFFSET_STORAGE_TOPIC": "eci_connect_offsets",
			"STATUS_STORAGE_TOPIC": "eci_connect_status",
		},
		ExposedPorts: []string{"8083/tcp"},
		Networks:     []string{networkName},
		WaitingFor: wait.ForHTTP("/connectors").WithPort("8083/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
			WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("avvio container Debezium bloccato per digest: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Terminate(ctx); err != nil {
			t.Logf("terminazione container kafka-connect: %v", err)
		}
	})
	return c
}

func kafkaConnectURL(t *testing.T, ctx context.Context, kafkaConnect testcontainers.Container) string {
	t.Helper()
	host, err := kafkaConnect.Host(ctx)
	if err != nil {
		t.Fatalf("kafka-connect Host: %v", err)
	}
	port, err := kafkaConnect.MappedPort(ctx, "8083/tcp")
	if err != nil {
		t.Fatalf("kafka-connect MappedPort: %v", err)
	}
	return fmt.Sprintf("http://%s:%s", host, port.Port())
}

// ============================================================
// Configurazione del connector: template reale, non duplicato a mano
// (SPEC-008 §2/§9).
// ============================================================

type connectorDoc struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config"`
}

func buildConnectorConfig(t *testing.T) []byte {
	t.Helper()
	path := repoPath(t, "deploy", "compose", "debezium-outbox-connector.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lettura template connector %s: %v", path, err)
	}
	var doc connectorDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parsing template connector: %v", err)
	}
	if doc.Name != connectorName {
		t.Fatalf("template connector: name = %q, want %q (constante connectorName disallineata dal file reale)", doc.Name, connectorName)
	}
	// Uniche sostituzioni: host/porta Postgres di QUESTO test (non quelli
	// fissi 'postgres'/'5432' dello stack compose dev). Tutti gli altri
	// campi (table.include.list, plugin.name, slot.name, publication.name,
	// transforms.outbox.*) restano quelli del file reale, non duplicati.
	doc.Config["database.hostname"] = postgresAlias
	doc.Config["database.port"] = postgresInternalPort

	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshalling connector config patchata: %v", err)
	}
	return out
}

// ============================================================
// Registrazione + attesa RUNNING: stessa logica a due fasi di
// deploy/compose/register-connector.sh (SPEC-007), replicata in Go.
// ============================================================

func registerConnector(t *testing.T, ctx context.Context, connectURL string, connectorJSON []byte) {
	t.Helper()
	deadline := time.Now().Add(registerTimeout)
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, connectURL+"/connectors", bytes.NewReader(connectorJSON))
		if err != nil {
			t.Fatalf("costruzione richiesta di registrazione: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusCreated || resp.StatusCode == http.StatusConflict {
				t.Logf("registrazione connector: http_code=%d (%s)", resp.StatusCode, string(body))
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("registrazione connector fallita dopo %s: http_code=%d body=%s", registerTimeout, resp.StatusCode, body)
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("registrazione connector fallita dopo %s: %v", registerTimeout, err)
		}
		time.Sleep(pollInterval)
	}
}

type connectorStatus struct {
	Connector struct {
		State string `json:"state"`
	} `json:"connector"`
	Tasks []struct {
		State string `json:"state"`
	} `json:"tasks"`
}

// waitConnectorRunning replica la fase 2 di register-connector.sh: il
// 201/409 della registrazione non basta, in modalità distribuita
// l'assegnazione del task richiede un rebalance di alcuni secondi
// (verificato empiricamente in SPEC-007 §10).
func waitConnectorRunning(t *testing.T, ctx context.Context, connectURL string) {
	t.Helper()
	deadline := time.Now().Add(runningTimeout)
	var last connectorStatus
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectURL+"/connectors/"+connectorName+"/status", nil)
		if err == nil {
			if resp, err2 := http.DefaultClient.Do(req); err2 == nil {
				body, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				var status connectorStatus
				if json.Unmarshal(body, &status) == nil {
					last = status
					runningTasks := 0
					for _, task := range status.Tasks {
						if task.State == "RUNNING" {
							runningTasks++
						}
					}
					if status.Connector.State == "RUNNING" && runningTasks >= 1 {
						return
					}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("connector %s non arrivato a RUNNING con almeno un task RUNNING entro %s: ultimo stato osservato connector.state=%q tasks=%+v",
				connectorName, runningTimeout, last.Connector.State, last.Tasks)
		}
		time.Sleep(pollInterval)
	}
}

// ============================================================
// Postgres: INSERT nella tabella outbox.
// ============================================================

func insertOutboxRow(t *testing.T, ctx context.Context, db *sql.DB, id, aggregateType, aggregateID, eventType, payload string) int64 {
	t.Helper()
	var sequence int64
	err := db.QueryRowContext(ctx, `
		INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, payload)
		VALUES ($1, $2, $3, $4, $5::jsonb)
		RETURNING event_sequence`,
		id, aggregateType, aggregateID, eventType, payload).Scan(&sequence)
	if err != nil {
		t.Fatalf("INSERT INTO outbox (aggregate_type=%s, aggregate_id=%s): %v", aggregateType, aggregateID, err)
	}
	return sequence
}

// ============================================================
// Kafka: consumo messaggi via Exec dentro il container
// (kafka-console-consumer.sh), non da un client Kafka sull'host.
// ============================================================

// tryConsumeOne prova a leggere esattamente un messaggio da topic entro
// timeout. ok=false se nessun messaggio arriva entro il timeout (topic
// non ancora esistente incluso — SPEC-008 §4: il consumer nativo gestisce
// da solo l'attesa/poll sulla creazione lazy del topic, non è un errore da
// diagnosticare qui).
func tryConsumeOne(t *testing.T, ctx context.Context, kafka testcontainers.Container, topic string, timeout time.Duration) (key, value string, ok bool) {
	t.Helper()
	cmd := []string{
		"/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", "localhost:9092",
		"--topic", topic,
		"--from-beginning",
		"--max-messages", "1",
		"--timeout-ms", strconv.Itoa(int(timeout.Milliseconds())),
		"--property", "print.key=true",
		"--property", "key.separator=|",
	}
	// Multiplexed(): senza, Exec ritorna lo stream Docker grezzo (header di
	// frame da 8 byte per chunk: 1 byte stream-type + 3 padding + 4 byte
	// lunghezza big-endian) invece dello stdout demux-ato — osservato
	// empiricamente come prefisso binario prima del JSON (bug del
	// harness, non di Debezium/Kafka Connect).
	_, reader, err := kafka.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("Exec kafka-console-consumer.sh su %s: %v", topic, err)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(reader); err != nil {
		t.Fatalf("lettura output kafka-console-consumer.sh su %s: %v", topic, err)
	}

	for _, line := range strings.Split(buf.String(), "\n") {
		line = strings.TrimRight(line, "\r")
		if !strings.Contains(line, "|") {
			continue
		}
		parts := strings.SplitN(line, "|", 2)
		// Riga plausibile chiave|valore solo se il valore sembra JSON
		// (evita di scambiare righe di log del tool per il messaggio).
		if len(parts) == 2 && strings.HasPrefix(strings.TrimSpace(parts[1]), "{") {
			return parts[0], parts[1], true
		}
	}
	return "", "", false
}

func tryConsumeOneWithHeaders(t *testing.T, ctx context.Context, kafka testcontainers.Container, topic string, timeout time.Duration) (headers, key, value string, ok bool) {
	t.Helper()
	cmd := []string{
		"/opt/kafka/bin/kafka-console-consumer.sh",
		"--bootstrap-server", "localhost:9092",
		"--topic", topic,
		"--from-beginning",
		"--max-messages", "1",
		"--timeout-ms", strconv.Itoa(int(timeout.Milliseconds())),
		"--property", "print.headers=true",
		"--property", "headers.separator=,",
		"--property", "headers.key.separator=:",
		"--property", "print.key=true",
		"--property", "key.separator=|",
	}
	_, reader, err := kafka.Exec(ctx, cmd, tcexec.Multiplexed())
	if err != nil {
		t.Fatalf("Exec kafka-console-consumer.sh con header su %s: %v", topic, err)
	}
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(reader); err != nil {
		t.Fatalf("lettura consumer con header su %s: %v", topic, err)
	}
	for _, line := range strings.Split(buf.String(), "\n") {
		parts := strings.SplitN(strings.TrimRight(line, "\r"), "|", 3)
		if len(parts) == 3 && strings.HasPrefix(strings.TrimSpace(parts[2]), "{") {
			return parts[0], parts[1], parts[2], true
		}
	}
	return "", "", "", false
}

// decodeJSONString decodifica un letterale stringa JSON (es. `"foo"`) nel
// suo valore Go semplice (`foo`). Con key.converter.schemas.enable=false
// (SPEC-008 §10), JsonConverter continua a codificare un valore STRING
// come letterale JSON con virgolette — comportamento standard, non un
// envelope residuo.
func decodeJSONString(t *testing.T, raw string) string {
	t.Helper()
	var s string
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		t.Fatalf("chiave del messaggio non è un letterale stringa JSON valido: %v (raw: %q)", err, raw)
	}
	return s
}

func assertJSONEqual(t *testing.T, got, want string) {
	t.Helper()
	var gotVal, wantVal any
	if err := json.Unmarshal([]byte(got), &gotVal); err != nil {
		t.Fatalf("valore del messaggio non è JSON valido: %v (raw: %q)", err, got)
	}
	if err := json.Unmarshal([]byte(want), &wantVal); err != nil {
		t.Fatalf("payload atteso non è JSON valido (bug nel test): %v", err)
	}
	gotNorm, _ := json.Marshal(gotVal)
	wantNorm, _ := json.Marshal(wantVal)
	if string(gotNorm) != string(wantNorm) {
		t.Errorf("valore del messaggio = %s, want %s (contenuto della colonna payload, non la riga outbox intera né l'envelope Debezium grezzo)", gotNorm, wantNorm)
	}
}

func repoPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// tests/integration/outbox_cdc/outbox_cdc_test.go -> repo root 3 livelli sopra.
	root := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	return filepath.Join(append([]string{root}, parts...)...)
}
