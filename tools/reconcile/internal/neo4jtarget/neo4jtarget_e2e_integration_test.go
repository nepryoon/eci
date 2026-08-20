//go:build integration

// SPEC-038 §3 scenario 3 — prova diretta che la ripubblicazione chiude
// DAVVERO il cerchio, non solo che scrive una riga in una tabella: lascio
// che il connector Debezium/Kafka reale instradi la riga outbox scritta da
// Republish verso sink-graph (T1.3), esattamente come farebbe per
// qualunque scrittura normale, e verifico che il nodo Neo4j mancante venga
// creato correttamente.
//
// Stack a 4 container (Postgres+Neo4j+Kafka+Kafka-Connect) più pesante
// degli scenari 1/2/4/5 (neo4jtarget_integration_test.go) — stesso
// principio già accettato altrove nel progetto quando la prova end-to-end
// lo giustifica (SPEC-038 §7, SPEC-008/SPEC-019). Pattern Postgres+Kafka+
// Kafka-Connect+registrazione connector ripreso da
// tests/integration/outbox_cdc/outbox_cdc_test.go (SPEC-008); pattern
// Neo4j+schema.cypher da consumer_integration_test.go (SPEC-015);
// pattern "sink-graph compilato ed eseguito come sottoprocesso Go reale,
// puntato sullo stack di test via env" da tests/e2e/conftest.py (SPEC-019,
// T1.6) — nessuno di questi tre reinventato qui, solo composti.
package neo4jtarget_test

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

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/neo4jtarget"
)

const (
	e2ePostgresAlias    = "reconcile-e2e-postgres"
	e2eKafkaAlias       = "reconcile-e2e-kafka"
	e2ePostgresPort     = "5432"
	e2eConnectorName    = "eci-outbox-connector"
	e2eRegisterTimeout  = 60 * time.Second
	e2eRunningTimeout   = 60 * time.Second
	e2ePollInterval     = 3 * time.Second
	e2eNeo4jPassword    = "eci-test-password-1234"
	e2eNodeWaitTimeout  = 60 * time.Second
	e2eNodeWaitInterval = 2 * time.Second
	// user/password/dbname DEVONO coincidere con quelli già scritti (non
	// sostituiti) in deploy/compose/debezium-outbox-connector.json —
	// buildConnectorConfig patcha SOLO database.hostname/database.port
	// (stesso principio di SPEC-008, tests/integration/outbox_cdc). NON gli
	// stessi dbUser/dbPassword/dbName del file sibling
	// neo4jtarget_integration_test.go (quelli sono arbitrari, non vincolati
	// da un template esterno).
	e2eDBUser     = "eci"
	e2eDBPassword = "eci-dev-only"
	e2eDBName     = "eci"
)

// Scenario 3 — vedi commento di package sopra.
func TestNeo4jTargetScenario3EndToEndDebeziumSinkGraph(t *testing.T) {
	ctx := context.Background()
	st := setupE2EStack(t, ctx)

	id := uniqueID(t, "scenario3")
	astHash := hash64("scenario3")
	insertCodeNodeRow(t, ctx, st.db, id, "Function", "ReconciledEndToEnd", astHash, "reconcile_e2e.go")
	// Nessun nodo Neo4j creato: simula un evento CodeNode perso, esattamente
	// come lo scenario 2 — qui la ripubblicazione viene però lasciata
	// fluire attraverso il connector Debezium/Kafka reale e sink-graph reale
	// (avviato in setupE2EStack), non solo verificata sulla riga outbox.

	target := neo4jtarget.New(st.neo4jDriver, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (id=%s mancante in Neo4j)", id)
	}

	waitForNeo4jNode(t, ctx, st.neo4jDriver, id, astHash, e2eNodeWaitTimeout)
}

// ============================================================
// Harness: Postgres+Neo4j+Kafka+Kafka-Connect su rete dedicata, più
// sink-graph reale come sottoprocesso Go.
// ============================================================

type e2eStack struct {
	db          *sql.DB
	neo4jDriver neo4j.DriverWithContext
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

	neo4jDriver, neo4jBoltURL := startE2ENeo4j(t, ctx)

	kafkaBrokers := startE2EKafka(t, ctx, net.Name)
	kafkaConnect := startKafkaConnect(t, ctx, net.Name)
	connectURL := kafkaConnectURL(t, ctx, kafkaConnect)
	connectorJSON := buildConnectorConfig(t, e2ePostgresAlias, e2ePostgresPort)
	registerConnector(t, ctx, connectURL, connectorJSON)
	waitConnectorRunning(t, ctx, connectURL)

	startSinkGraphProcess(t, postgresDSN, neo4jBoltURL, kafkaBrokers)

	return &e2eStack{db: db, neo4jDriver: neo4jDriver}
}

func startE2EPostgres(t *testing.T, ctx context.Context, networkName string) string {
	t.Helper()
	container, err := tcpostgres.Run(ctx, "postgres:17",
		tcpostgres.WithUsername(e2eDBUser),
		tcpostgres.WithPassword(e2eDBPassword),
		tcpostgres.WithDatabase(e2eDBName),
		tcpostgres.BasicWaitStrategies(),
		// wal_level=logical (append via WithCmdArgs: il modulo tcpostgres
		// imposta già Cmd=["postgres","-c","fsync=off"] di default, SPEC-008
		// §2 richiede logical decoding per Debezium, non sostituibile senza
		// perdere fsync=off) — max_wal_senders/max_replication_slots stessa
		// configurazione di tests/integration/outbox_cdc/outbox_cdc_test.go.
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

func startE2ENeo4j(t *testing.T, ctx context.Context) (neo4j.DriverWithContext, string) {
	t.Helper()
	container, err := tcneo4j.Run(ctx, "neo4j:5-community", tcneo4j.WithAdminPassword(e2eNeo4jPassword))
	if err != nil {
		t.Fatalf("avvio container neo4j:5-community: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container neo4j: %v", err)
		}
	})
	boltURL, err := container.BoltUrl(ctx)
	if err != nil {
		t.Fatalf("BoltUrl: %v", err)
	}
	driver, err := neo4j.NewDriverWithContext(boltURL, neo4j.BasicAuth("neo4j", e2eNeo4jPassword, ""))
	if err != nil {
		t.Fatalf("driver neo4j: %v", err)
	}
	t.Cleanup(func() { _ = driver.Close(ctx) })

	applyNeo4jSchema(t, ctx, driver)

	return driver, boltURL
}

// applyNeo4jSchema applica contracts/cypher/schema.cypher (D3) per davvero,
// stesso principio di consumer_integration_test.go (SPEC-015 §7): sink-graph
// scrive rispettando quei constraint, un test che non li applica non
// eserciterebbe il percorso reale.
func applyNeo4jSchema(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext) {
	t.Helper()
	src := readRepoFile(t, "contracts", "cypher", "schema.cypher")
	session := driver.NewSession(ctx, neo4j.SessionConfig{})
	defer session.Close(ctx)
	for _, stmt := range splitCypherStatements(src) {
		if _, err := session.Run(ctx, stmt, nil); err != nil {
			t.Fatalf("applicazione schema.cypher, statement %q: %v", firstLine(stmt), err)
		}
	}
}

func startE2EKafka(t *testing.T, ctx context.Context, networkName string) []string {
	t.Helper()
	// confluent-local, stesso pattern di consumer_integration_test.go
	// (SPEC-015 §10): risolve advertised.listeners per un consumer/produttore
	// reale dal processo host (qui: il sottoprocesso sink-graph), MA
	// aggiunto un alias di rete esplicito perché Kafka Connect (container)
	// deve raggiungerlo internamente, cosa che consumer_integration_test.go
	// non richiedeva (nessun Kafka Connect in quel test).
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

	// Pre-creazione ESPLICITA dei topic PRIMA che sink-graph formi il
	// proprio consumer group (startSinkGraphProcess, dopo) — stesso
	// principio di consumer_integration_test.go (SPEC-015 §10): un consumer
	// group con GroupTopics che si unisce PRIMA che un topic esista riceve
	// zero partizioni in quell'assegnazione iniziale e non le riscopre da
	// solo quando Debezium lo crea più tardi (lazy auto-create al primo
	// evento CDC) — nessun rebalance automatico lo attiva. Scoperto
	// scrivendo questo stesso test: sink-graph si univa al gruppo, otteneva
	// l'assegnazione, e restava silenziosamente senza mai ricevere nulla.
	ensureKafkaTopics(t, ctx, brokers, "outbox.event.CodeNode", "outbox.event.CodeRelation")

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
		// debezium/connect:latest non esiste su Docker Hub (SPEC-007 §10) —
		// quay.io/debezium/connect:latest riusato qui, stesso di SPEC-008.
		Image: "quay.io/debezium/connect:latest",
		Env: map[string]string{
			"BOOTSTRAP_SERVERS":    e2eKafkaAlias + ":9092",
			"GROUP_ID":             "reconcile-e2e-connect",
			"CONFIG_STORAGE_TOPIC": "reconcile_e2e_connect_configs",
			"OFFSET_STORAGE_TOPIC": "reconcile_e2e_connect_offsets",
			"STATUS_STORAGE_TOPIC": "reconcile_e2e_connect_status",
		},
		ExposedPorts: []string{"8083/tcp"},
		Networks:     []string{networkName},
		WaitingFor: wait.ForHTTP("/connectors").WithPort("8083/tcp").
			WithStatusCodeMatcher(func(status int) bool { return status == 200 }).
			WithStartupTimeout(90 * time.Second),
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: req, Started: true})
	if err != nil {
		t.Fatalf("avvio container quay.io/debezium/connect:latest: %v", err)
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
// sink-graph (T1.3) come sottoprocesso Go reale — stesso pattern di
// tests/e2e/conftest.py `_build_go_binary`/`sink_graph_process` (SPEC-019,
// T1.6): compilato ed eseguito puntato sullo stack di test via env, non
// reimplementato/mockato.
// ============================================================

func startSinkGraphProcess(t *testing.T, postgresDSN, neo4jBoltURL string, kafkaBrokers []string) {
	t.Helper()
	sinkGraphDir := repoPath(t, "services", "sink-graph")
	binary := buildGoBinary(t, sinkGraphDir, "sink-graph")

	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(),
		"NEO4J_URI="+neo4jBoltURL,
		"NEO4J_USER=neo4j",
		"NEO4J_PASSWORD="+e2eNeo4jPassword,
		"POSTGRES_DSN="+postgresDSN,
		"KAFKA_BROKERS="+strings.Join(kafkaBrokers, ","),
		"METRICS_PORT="+freePort(t),
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("avvio sottoprocesso sink-graph: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Logf("output sink-graph:\n%s", output.String())
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
// Verifica finale: attesa propagazione CDC->sink-graph->Neo4j, mai un
// semplice sleep fisso (stesso principio di tests/e2e/harness.py, SPEC-019).
// ============================================================

func waitForNeo4jNode(t *testing.T, ctx context.Context, driver neo4j.DriverWithContext, id, wantAstHash string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		session := driver.NewSession(ctx, neo4j.SessionConfig{})
		result, err := session.Run(ctx, `MATCH (n {id: $id}) RETURN n.ast_hash AS ast_hash`, map[string]any{"id": id})
		if err == nil {
			var records []*neo4j.Record
			records, err = result.Collect(ctx)
			if err == nil && len(records) > 0 {
				astHash, _, fieldErr := neo4j.GetRecordValue[string](records[0], "ast_hash")
				session.Close(ctx)
				if fieldErr != nil {
					lastErr = fieldErr
				} else if astHash == wantAstHash {
					return
				} else {
					lastErr = fmt.Errorf("nodo id=%s presente ma ast_hash=%q, want %q (ancora in propagazione?)", id, astHash, wantAstHash)
				}
				time.Sleep(e2eNodeWaitInterval)
				continue
			}
		}
		session.Close(ctx)
		lastErr = err
		time.Sleep(e2eNodeWaitInterval)
	}
	t.Fatalf("nodo Neo4j id=%s con ast_hash=%q non comparso entro %s (propagazione CDC Postgres->Debezium->Kafka->sink-graph->Neo4j): ultimo errore/stato osservato: %v", id, wantAstHash, timeout, lastErr)
}

// ============================================================
// Utility di supporto riprese da tests/integration/outbox_cdc/outbox_cdc_test.go
// (SPEC-008) — piccole, duplicate localmente per lo stesso motivo già
// documentato altrove in questo progetto (nessun package condiviso tra
// moduli Go distinti per helper di test minori).
// ============================================================

func readRepoFile(t *testing.T, parts ...string) string {
	t.Helper()
	path := repoPath(t, parts...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("lettura %s: %v", path, err)
	}
	return string(data)
}

func splitCypherStatements(src string) []string {
	var kept []string
	for _, line := range strings.Split(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		kept = append(kept, line)
	}
	joined := strings.Join(kept, "\n")

	var statements []string
	for _, raw := range strings.Split(joined, ";") {
		stmt := strings.TrimSpace(raw)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

// logDiagnostics stampa i log del container solo quando il test è già
// fallito (t.Failed()): diagnostica per capire DOVE si è rotta la catena
// Postgres->Debezium->Kafka->sink-graph->Neo4j, non rumore su run verdi.
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

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}
