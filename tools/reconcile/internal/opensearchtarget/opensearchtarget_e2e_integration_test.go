//go:build integration

// SPEC-040 §3 scenario 3 — prova diretta che la ripubblicazione chiude
// DAVVERO il cerchio, non solo che scrive una riga in una tabella: lascio
// che il connector Debezium/Kafka reale instradi la riga outbox scritta da
// Republish verso sink-search (T3.2), esattamente come farebbe per
// qualunque scrittura normale, e verifico che il documento OpenSearch
// mancante venga creato correttamente — stesso principio degli scenari 3
// di SPEC-038/039
// (tools/reconcile/internal/neo4jtarget/neo4jtarget_e2e_integration_test.go,
// tools/reconcile/internal/qdranttarget/qdranttarget_e2e_integration_test.go),
// qui puntato su sink-search invece di sink-graph/sink-vector.
//
// Stack a 4 container (Postgres+OpenSearch+Kafka+Kafka-Connect) più
// pesante degli scenari 1/2/4/5 (opensearchtarget_integration_test.go) —
// stesso principio già accettato altrove nel progetto quando la prova
// end-to-end lo giustifica. Pattern Postgres+Kafka+Kafka-Connect+
// registrazione connector, e pattern "servizio compilato ed eseguito come
// sottoprocesso Go reale puntato sullo stack di test via env", ripresi
// identici dai file e2e di SPEC-038/039 — nessuno dei due reinventato
// qui, solo composti (stessa deviazione dichiarata da SPEC-038 §10 punto
// 7 sulle utility duplicate: nessun package condiviso tra moduli Go
// distinti per helper di test minori, replicata identica qui).
package opensearchtarget_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/opensearch-project/opensearch-go/v4"
	"github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	"github.com/opensearch-project/opensearch-go/v4/opensearchutil"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcopensearch "github.com/testcontainers/testcontainers-go/modules/opensearch"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/opensearchtarget"
)

const (
	e2ePostgresAlias   = "reconcile-e2e-postgres"
	e2eKafkaAlias      = "reconcile-e2e-kafka"
	e2ePostgresPort    = "5432"
	e2eConnectorName   = "eci-outbox-connector"
	e2eRegisterTimeout = 60 * time.Second
	e2eRunningTimeout  = 60 * time.Second
	e2ePollInterval    = 3 * time.Second
	e2eDocWaitTimeout  = 60 * time.Second
	e2eDocWaitInterval = 2 * time.Second
	// topicCodeChunk — STESSA costante di
	// services/sink-search/internal/consumer.TopicCodeChunk, non
	// importabile (modulo Go separato + pacchetto internal/, stesso
	// motivo di indexName in qdranttarget.go), replicata qui identica.
	topicCodeChunk = "outbox.event.CodeChunk"
	// user/password/dbname DEVONO coincidere con quelli già scritti (non
	// sostituiti) in deploy/compose/debezium-outbox-connector.json —
	// buildConnectorConfig patcha SOLO database.hostname/database.port
	// (stesso principio di SPEC-008/SPEC-038/SPEC-039). NON gli stessi
	// dbUser/dbPassword/dbName del file sibling
	// opensearchtarget_integration_test.go (quelli sono arbitrari, non
	// vincolati da un template esterno).
	e2eDBUser     = "eci"
	e2eDBPassword = "eci-dev-only"
	e2eDBName     = "eci"
)

// Scenario 3 — vedi commento di package sopra.
func TestOpenSearchTargetScenario3EndToEndDebeziumSinkSearch(t *testing.T) {
	ctx := context.Background()
	st := setupE2EStack(t, ctx)

	entityID := hash64("scenario3-entity")
	text := "func Scenario3EndToEnd() {}"
	chunkID := insertFullRow(t, ctx, st.db, entityID, text)
	// Nessun documento OpenSearch creato: simula un evento CodeChunk
	// perso, esattamente come lo scenario 2 — qui la ripubblicazione viene
	// però lasciata fluire attraverso il connector Debezium/Kafka reale e
	// sink-search reale (avviato in setupE2EStack), non solo verificata
	// sulla riga outbox.

	target := opensearchtarget.New(st.opensearch, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (chunk_id=%s mancante in OpenSearch)", chunkID)
	}

	waitForOpenSearchDocument(t, ctx, st.opensearch, chunkID, text, e2eDocWaitTimeout)
}

// ============================================================
// Harness: Postgres+OpenSearch+Kafka+Kafka-Connect su rete dedicata, più
// sink-search reale come sottoprocesso Go.
// ============================================================

type e2eStack struct {
	db         *sql.DB
	opensearch *opensearchapi.Client
}

func setupE2EStack(t *testing.T, ctx context.Context) *e2eStack {
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

	postgresDSN := startE2EPostgres(t, ctx, net.Name)
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("sql.Open postgres: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping postgres: %v", err)
	}
	applyPostgresMigrations(t, ctx, postgresDSN)

	openSearchClient, openSearchURL := startE2EOpenSearch(t, ctx)

	kafkaBrokers := startE2EKafka(t, ctx, net.Name)
	kafkaConnect := startKafkaConnect(t, ctx, net.Name)
	connectURL := kafkaConnectURL(t, ctx, kafkaConnect)
	connectorJSON := buildConnectorConfig(t, e2ePostgresAlias, e2ePostgresPort)
	registerConnector(t, ctx, connectURL, connectorJSON)
	waitConnectorRunning(t, ctx, connectURL)

	startSinkSearchProcess(t, postgresDSN, openSearchURL, kafkaBrokers)

	return &e2eStack{db: db, opensearch: openSearchClient}
}

func startE2EPostgres(t *testing.T, ctx context.Context, networkName string) string {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithUsername(e2eDBUser),
		tcpostgres.WithPassword(e2eDBPassword),
		tcpostgres.WithDatabase(e2eDBName),
		tcpostgres.BasicWaitStrategies(),
		// wal_level=logical (append via WithCmdArgs: il modulo tcpostgres
		// imposta già Cmd=["postgres","-c","fsync=off"] di default,
		// SPEC-008 §2 richiede logical decoding per Debezium, non
		// sostituibile senza perdere fsync=off) — max_wal_senders/
		// max_replication_slots stessa configurazione dei file e2e di
		// SPEC-038/039.
		testcontainers.WithCmdArgs("-c", "wal_level=logical", "-c", "max_wal_senders=4", "-c", "max_replication_slots=4"),
		tcnetwork.WithNetworkName([]string{e2ePostgresAlias}, networkName),
	)
	if err != nil {
		t.Fatalf("avvio container postgres:17: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container postgres: %v", err)
		}
	})
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("ConnectionString: %v", err)
	}
	return dsn
}

func applyPostgresMigrations(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	if _, err := exec.LookPath("migrate"); err != nil {
		t.Fatalf("binario 'migrate' non trovato sul PATH: %v", err)
	}
	migrationsDir := repoPath(t, "contracts", "sql", "migrations")
	cmd := exec.CommandContext(ctx, "migrate", "-source", "file://"+migrationsDir, "-database", dsn, "up")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("migrate up fallita: %v\noutput:\n%s", err, out)
	}
}

// startE2EOpenSearch avvia OpenSearch reale e crea SUBITO l'indice
// code_chunks (stesso mapping di startOpenSearch in
// opensearchtarget_integration_test.go) — necessario PRIMA che Reconcile
// chiami Check (Document.Get su un indice inesistente fallisce con un
// errore diverso da "documento assente"). sink-search, avviato DOPO,
// tenterà a propria volta EnsureIndex sull'indice già esistente: stesso
// comportamento idempotente già verificato da SPEC-034 scenario 4, nessun
// conflitto.
func startE2EOpenSearch(t *testing.T, ctx context.Context) (client *opensearchapi.Client, url string) {
	t.Helper()
	container, err := tcopensearch.Run(ctx, "opensearchproject/opensearch:2.11.1")
	if err != nil {
		t.Fatalf("avvio container opensearch: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container opensearch: %v", err)
		}
	})

	address, err := container.Address(ctx)
	if err != nil {
		t.Fatalf("opensearch Address: %v", err)
	}

	client, err = opensearchapi.NewClient(opensearchapi.Config{
		Client: opensearch.Config{Addresses: []string{address}},
	})
	if err != nil {
		t.Fatalf("opensearchapi.NewClient: %v", err)
	}

	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"text":        map[string]any{"type": "text"},
				"entity_id":   map[string]any{"type": "keyword"},
				"chunk_index": map[string]any{"type": "integer"},
			},
		},
	}
	if _, err := client.Indices.Create(ctx, opensearchapi.IndicesCreateReq{
		Index: indexName,
		Body:  opensearchutil.NewJSONReader(mapping),
	}); err != nil {
		t.Fatalf("Indices.Create(%s): %v", indexName, err)
	}

	return client, address
}

func startE2EKafka(t *testing.T, ctx context.Context, networkName string) []string {
	t.Helper()
	// confluent-local, stesso pattern dei file e2e di SPEC-038/039:
	// risolve advertised.listeners per un consumer/produttore reale dal
	// processo host (qui: il sottoprocesso sink-search), MA aggiunto un
	// alias di rete esplicito perché Kafka Connect (container) deve
	// raggiungerlo internamente.
	container, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tcnetwork.WithNetworkName([]string{e2eKafkaAlias}, networkName),
	)
	if err != nil {
		t.Fatalf("avvio container kafka: %v", err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			logDiagnostics(t, ctx, container, "kafka")
		}
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container kafka: %v", err)
		}
	})
	brokers, err := container.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka Brokers: %v", err)
	}

	// Pre-creazione ESPLICITA del topic PRIMA che sink-search formi il
	// proprio consumer group — stesso principio già stabilito dai file e2e
	// di SPEC-038/039: un consumer group con GroupTopics che si unisce
	// PRIMA che un topic esista riceve zero partizioni in quell'assegna-
	// zione iniziale e non le riscopre da solo quando Debezium lo crea più
	// tardi (lazy auto-create al primo evento CDC).
	ensureKafkaTopics(t, ctx, brokers,
		topicCodeChunk,
		topicCodeChunk+".retry.sink-search",
	)

	return brokers
}

func ensureKafkaTopics(t *testing.T, ctx context.Context, brokers []string, topics ...string) {
	t.Helper()
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial kafka per creazione topic: %v", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("kafka Controller: %v", err)
	}
	controllerAddr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := kafka.DialContext(ctx, "tcp", controllerAddr)
	if err != nil {
		t.Fatalf("dial kafka controller (%s): %v", controllerAddr, err)
	}
	defer controllerConn.Close()

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
	}
	if err := controllerConn.CreateTopics(configs...); err != nil {
		t.Fatalf("CreateTopics %v: %v", topics, err)
	}
}

func startKafkaConnect(t *testing.T, ctx context.Context, networkName string) testcontainers.Container {
	t.Helper()
	req := testcontainers.ContainerRequest{
		// debezium/connect:latest non esiste su Docker Hub (deviazione storica SPEC-007 §10)
		// — immagine ufficiale bloccata per digest, stessa di
		// SPEC-008/SPEC-038/SPEC-039.
		Image: "quay.io/debezium/connect@sha256:698f0559e667a242f962221079e75917b2b7a3ad4de62661e977628da0e33b45",
		Env: map[string]string{
			"BOOTSTRAP_SERVERS":    e2eKafkaAlias + ":9092",
			"GROUP_ID":             "reconcile-e2e-opensearch-connect",
			"CONFIG_STORAGE_TOPIC": "reconcile_e2e_opensearch_connect_configs",
			"OFFSET_STORAGE_TOPIC": "reconcile_e2e_opensearch_connect_offsets",
			"STATUS_STORAGE_TOPIC": "reconcile_e2e_opensearch_connect_status",
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
		if t.Failed() {
			logDiagnostics(t, ctx, c, "kafka-connect")
		}
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
// Configurazione del connector: STESSO template reale di SPEC-008
// (deploy/compose/debezium-outbox-connector.json), non duplicato a mano.
// ============================================================

type connectorDoc struct {
	Name   string         `json:"name"`
	Config map[string]any `json:"config"`
}

func buildConnectorConfig(t *testing.T, postgresHost, postgresPort string) []byte {
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
	if doc.Name != e2eConnectorName {
		t.Fatalf("template connector: name = %q, want %q (constante e2eConnectorName disallineata dal file reale)", doc.Name, e2eConnectorName)
	}
	doc.Config["database.hostname"] = postgresHost
	doc.Config["database.port"] = postgresPort

	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshalling connector config patchata: %v", err)
	}
	return out
}

func registerConnector(t *testing.T, ctx context.Context, connectURL string, connectorJSON []byte) {
	t.Helper()
	deadline := time.Now().Add(e2eRegisterTimeout)
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
				t.Fatalf("registrazione connector fallita dopo %s: http_code=%d body=%s", e2eRegisterTimeout, resp.StatusCode, body)
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("registrazione connector fallita dopo %s: %v", e2eRegisterTimeout, err)
		}
		time.Sleep(e2ePollInterval)
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

func waitConnectorRunning(t *testing.T, ctx context.Context, connectURL string) {
	t.Helper()
	deadline := time.Now().Add(e2eRunningTimeout)
	var last connectorStatus
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, connectURL+"/connectors/"+e2eConnectorName+"/status", nil)
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
				e2eConnectorName, e2eRunningTimeout, last.Connector.State, last.Tasks)
		}
		time.Sleep(e2ePollInterval)
	}
}

// ============================================================
// sink-search (T3.2) come sottoprocesso Go reale — stesso pattern di
// startSinkGraphProcess/startSinkVectorProcess nei file e2e di
// SPEC-038/039: compilato ed eseguito puntato sullo stack di test via
// env, non reimplementato/mockato.
// ============================================================

func startSinkSearchProcess(t *testing.T, postgresDSN, openSearchURL string, kafkaBrokers []string) {
	t.Helper()
	sinkSearchDir := repoPath(t, "services", "sink-search")
	binary := buildGoBinary(t, sinkSearchDir, "sink-search")

	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(),
		"POSTGRES_DSN="+postgresDSN,
		"OPENSEARCH_URL="+openSearchURL,
		"KAFKA_BROKERS="+strings.Join(kafkaBrokers, ","),
		"METRICS_PORT="+freePort(t),
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("avvio sottoprocesso sink-search: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Logf("output sink-search:\n%s", output.String())
	})
}

func buildGoBinary(t *testing.T, dir, name string) string {
	t.Helper()
	outPath := filepath.Join(t.TempDir(), name)
	cmd := exec.Command("go", "build", "-o", outPath, ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s (dir=%s): %v\noutput:\n%s", name, dir, err, out)
	}
	return outPath
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind porta libera: %v", err)
	}
	defer l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split porta libera: %v", err)
	}
	return port
}

// ============================================================
// Verifica finale: attesa propagazione CDC->sink-search->OpenSearch, mai
// un semplice sleep fisso (stesso principio dei file e2e di SPEC-038/
// 039).
// ============================================================

func waitForOpenSearchDocument(t *testing.T, ctx context.Context, client *opensearchapi.Client, chunkID, wantText string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		resp, err := client.Document.Get(ctx, opensearchapi.DocumentGetReq{
			Index:      indexName,
			DocumentID: chunkID,
		})
		if err == nil && resp.Found {
			var source struct {
				Text string `json:"text"`
			}
			if decodeErr := json.Unmarshal(resp.Source, &source); decodeErr != nil {
				lastErr = decodeErr
			} else if source.Text == wantText {
				return
			} else {
				lastErr = fmt.Errorf("documento id=%s presente ma text=%q, want %q (ancora in propagazione?)", chunkID, source.Text, wantText)
			}
		} else if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("documento id=%s non ancora presente", chunkID)
		}
		time.Sleep(e2eDocWaitInterval)
	}
	t.Fatalf("documento OpenSearch id=%s con text=%q non comparso entro %s (propagazione CDC Postgres->Debezium->Kafka->sink-search->OpenSearch): ultimo errore/stato osservato: %v",
		chunkID, wantText, timeout, lastErr)
}

// ============================================================
// logDiagnostics stampa i log del container solo quando il test è già
// fallito (t.Failed()) — diagnostica per capire DOVE si è rotta la catena
// Postgres->Debezium->Kafka->sink-search->OpenSearch, non rumore su run
// verdi.
// ============================================================

func logDiagnostics(t *testing.T, ctx context.Context, c testcontainers.Container, label string) {
	t.Helper()
	logs, err := c.Logs(ctx)
	if err != nil {
		t.Logf("log %s non disponibili: %v", label, err)
		return
	}
	defer logs.Close()
	data, err := io.ReadAll(logs)
	if err != nil {
		t.Logf("lettura log %s fallita: %v", label, err)
		return
	}
	t.Logf("log %s:\n%s", label, data)
}
