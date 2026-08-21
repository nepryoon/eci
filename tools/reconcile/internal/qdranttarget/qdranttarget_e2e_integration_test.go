//go:build integration

// SPEC-039 §3 scenario 3 — prova diretta che la ripubblicazione chiude
// DAVVERO il cerchio, non solo che scrive una riga in una tabella: lascio
// che il connector Debezium/Kafka reale instradi la riga outbox scritta da
// Republish verso sink-vector (T3.1), esattamente come farebbe per
// qualunque scrittura normale, e verifico che il punto Qdrant mancante
// venga creato correttamente — stesso principio dello scenario 3 di
// SPEC-038 (tools/reconcile/internal/neo4jtarget/neo4jtarget_e2e_integration_test.go),
// qui puntato su sink-vector invece di sink-graph.
//
// Stack a 4 container (Postgres+Qdrant+Kafka+Kafka-Connect) più pesante
// degli scenari 1/2/4/5/6 (qdranttarget_integration_test.go) — stesso
// principio già accettato altrove nel progetto quando la prova end-to-end
// lo giustifica. Pattern Postgres+Kafka+Kafka-Connect+registrazione
// connector, e pattern "servizio compilato ed eseguito come sottoprocesso
// Go reale puntato sullo stack di test via env", ripresi identici da
// neo4jtarget_e2e_integration_test.go (SPEC-038) — nessuno dei due
// reinventato qui, solo composti (stessa deviazione dichiarata da SPEC-038
// §10 punto 7 sulle utility duplicate: nessun package condiviso tra moduli
// Go distinti per helper di test minori, replicata identica qui).
package qdranttarget_test

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
	"github.com/qdrant/go-client/qdrant"
	kafka "github.com/segmentio/kafka-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcqdrant "github.com/testcontainers/testcontainers-go/modules/qdrant"

	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/eci-project/eci/tools/reconcile/internal/framework"
	"github.com/eci-project/eci/tools/reconcile/internal/qdranttarget"
)

const (
	e2ePostgresAlias     = "reconcile-e2e-postgres"
	e2eKafkaAlias        = "reconcile-e2e-kafka"
	e2ePostgresPort      = "5432"
	e2eConnectorName     = "eci-outbox-connector"
	e2eRegisterTimeout   = 60 * time.Second
	e2eRunningTimeout    = 60 * time.Second
	e2ePollInterval      = 3 * time.Second
	e2ePointWaitTimeout  = 60 * time.Second
	e2ePointWaitInterval = 2 * time.Second
	// topicCodeEmbedding — STESSA costante di
	// services/sink-vector/internal/consumer.TopicCodeEmbedding, non
	// importabile (modulo Go separato + pacchetto internal/, stesso motivo
	// di collectionName/pointIDNamespace in qdranttarget.go), replicata qui
	// identica.
	topicCodeEmbedding = "outbox.event.CodeEmbedding"
	// user/password/dbname DEVONO coincidere con quelli già scritti (non
	// sostituiti) in deploy/compose/debezium-outbox-connector.json —
	// buildConnectorConfig patcha SOLO database.hostname/database.port
	// (stesso principio di SPEC-008/SPEC-038). NON gli stessi
	// dbUser/dbPassword/dbName del file sibling
	// qdranttarget_integration_test.go (quelli sono arbitrari, non
	// vincolati da un template esterno).
	e2eDBUser     = "eci"
	e2eDBPassword = "eci-dev-only"
	e2eDBName     = "eci"
)

// Scenario 3 — vedi commento di package sopra.
func TestQdrantTargetScenario3EndToEndDebeziumSinkVector(t *testing.T) {
	ctx := context.Background()
	st := setupE2EStack(t, ctx)

	entityID := hash64("scenario3-entity")
	// fullVector (1536 dim), NON smallVector: a differenza degli scenari
	// 1/2/4/5/6 (qdranttarget_integration_test.go, dove il vettore
	// Postgres non viene MAI upsertato realmente su Qdrant da
	// qdranttarget stesso), qui è sink-vector REALE a leggere
	// code_embedding.vector e a scriverlo su Qdrant — la collection
	// code_embeddings richiede sempre Size=1536 esatte per qualunque
	// upsert (SPEC-033 §2): un vettore più corto viene rifiutato dal
	// SERVER Qdrant, e con resilience.WithRetryAndDLQ (SPEC-035) quel
	// fallimento non produce ALCUN log visibile (l'errore viene
	// convertito in retry silenziosi poi DLQ, mai propagato al chiamante
	// come errore) — scoperto proprio scrivendo questo test (diagnosticato
	// con Count/Scroll su Qdrant dopo il timeout: 0 punti scritti nonostante
	// nessun errore in log).
	embeddingID := insertFullRow(t, ctx, st.db, entityID, fullVector(3))
	// Nessun punto Qdrant creato: simula un evento CodeEmbedding perso,
	// esattamente come lo scenario 2 — qui la ripubblicazione viene però
	// lasciata fluire attraverso il connector Debezium/Kafka reale e
	// sink-vector reale (avviato in setupE2EStack), non solo verificata
	// sulla riga outbox.

	target := qdranttarget.New(st.qdrant, st.db)
	report, err := framework.Reconcile(ctx, st.db, target)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if report.Republished == 0 {
		t.Fatalf("report.Republished = 0, want >0 (embedding_id=%s mancante in Qdrant)", embeddingID)
	}

	waitForQdrantPoint(t, ctx, st.qdrant, embeddingID, entityID, e2ePointWaitTimeout)
}

// ============================================================
// Harness: Postgres+Qdrant+Kafka+Kafka-Connect su rete dedicata, più
// sink-vector reale come sottoprocesso Go.
// ============================================================

type e2eStack struct {
	db     *sql.DB
	qdrant *qdrant.Client
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

	qdrantClient, qdrantHost, qdrantPort := startE2EQdrant(t, ctx)

	kafkaBrokers := startE2EKafka(t, ctx, net.Name)
	kafkaConnect := startKafkaConnect(t, ctx, net.Name)
	connectURL := kafkaConnectURL(t, ctx, kafkaConnect)
	connectorJSON := buildConnectorConfig(t, e2ePostgresAlias, e2ePostgresPort)
	registerConnector(t, ctx, connectURL, connectorJSON)
	waitConnectorRunning(t, ctx, connectURL)

	startSinkVectorProcess(t, postgresDSN, qdrantHost, qdrantPort, kafkaBrokers)

	return &e2eStack{db: db, qdrant: qdrantClient}
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
		// configurazione di neo4jtarget_e2e_integration_test.go.
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

// startE2EQdrant avvia Qdrant reale e crea SUBITO la collection
// code_embeddings (Size=1536/Distance=Cosine, stessa config di
// startQdrant in qdranttarget_integration_test.go) — necessaria PRIMA che
// Reconcile chiami Check (Get su una collection inesistente fallisce con
// un errore, diverso da "punto assente"). sink-vector, avviato DOPO,
// tenterà a propria volta EnsureCollection sulla collection già esistente:
// stesso comportamento idempotente già verificato da SPEC-033 scenario 4,
// nessun conflitto.
func startE2EQdrant(t *testing.T, ctx context.Context) (client *qdrant.Client, host string, port int) {
	t.Helper()
	container, err := tcqdrant.Run(ctx, "qdrant/qdrant:v1.15.1")
	if err != nil {
		t.Fatalf("avvio container qdrant: %v", err)
	}
	t.Cleanup(func() {
		if err := container.Terminate(ctx); err != nil {
			t.Logf("terminazione container qdrant: %v", err)
		}
	})

	host, err = container.Host(ctx)
	if err != nil {
		t.Fatalf("qdrant Host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, "6334/tcp")
	if err != nil {
		t.Fatalf("qdrant MappedPort 6334/tcp: %v", err)
	}
	port = int(mappedPort.Num())

	client, err = qdrant.NewClient(&qdrant.Config{Host: host, Port: port})
	if err != nil {
		t.Fatalf("qdrant NewClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	if err := client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collectionName,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     vectorSize,
			Distance: qdrant.Distance_Cosine,
		}),
	}); err != nil {
		t.Fatalf("CreateCollection(%s): %v", collectionName, err)
	}

	return client, host, port
}

func startE2EKafka(t *testing.T, ctx context.Context, networkName string) []string {
	t.Helper()
	// confluent-local, stesso pattern di neo4jtarget_e2e_integration_test.go
	// (SPEC-038 §10 punto 7): risolve advertised.listeners per un
	// consumer/produttore reale dal processo host (qui: il sottoprocesso
	// sink-vector), MA aggiunto un alias di rete esplicito perché Kafka
	// Connect (container) deve raggiungerlo internamente.
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

	// Pre-creazione ESPLICITA del topic PRIMA che sink-vector formi il
	// proprio consumer group — stesso principio già stabilito da
	// consumer_integration_test.go (sink-vector) e
	// neo4jtarget_e2e_integration_test.go (SPEC-038 §10 punto 7): un
	// consumer group con GroupTopics che si unisce PRIMA che un topic
	// esista riceve zero partizioni in quell'assegnazione iniziale e non le
	// riscopre da solo quando Debezium lo crea più tardi (lazy auto-create
	// al primo evento CDC).
	ensureKafkaTopics(t, ctx, brokers, topicCodeEmbedding)

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
		// quay.io/debezium/connect:latest riusato qui, stesso di SPEC-008/
		// SPEC-038.
		Image: "quay.io/debezium/connect:latest",
		Env: map[string]string{
			"BOOTSTRAP_SERVERS":    e2eKafkaAlias + ":9092",
			"GROUP_ID":             "reconcile-e2e-qdrant-connect",
			"CONFIG_STORAGE_TOPIC": "reconcile_e2e_qdrant_connect_configs",
			"OFFSET_STORAGE_TOPIC": "reconcile_e2e_qdrant_connect_offsets",
			"STATUS_STORAGE_TOPIC": "reconcile_e2e_qdrant_connect_status",
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
// sink-vector (T3.1) come sottoprocesso Go reale — stesso pattern di
// startSinkGraphProcess in neo4jtarget_e2e_integration_test.go (SPEC-038):
// compilato ed eseguito puntato sullo stack di test via env, non
// reimplementato/mockato.
// ============================================================

func startSinkVectorProcess(t *testing.T, postgresDSN, qdrantHost string, qdrantPort int, kafkaBrokers []string) {
	t.Helper()
	sinkVectorDir := repoPath(t, "services", "sink-vector")
	binary := buildGoBinary(t, sinkVectorDir, "sink-vector")

	cmd := exec.Command(binary)
	cmd.Env = append(os.Environ(),
		"POSTGRES_DSN="+postgresDSN,
		"QDRANT_HOST="+qdrantHost,
		fmt.Sprintf("QDRANT_GRPC_PORT=%d", qdrantPort),
		"KAFKA_BROKERS="+strings.Join(kafkaBrokers, ","),
		"METRICS_PORT="+freePort(t),
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output

	if err := cmd.Start(); err != nil {
		t.Fatalf("avvio sottoprocesso sink-vector: %v", err)
	}
	t.Cleanup(func() {
		if cmd.Process == nil {
			return
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Logf("output sink-vector:\n%s", output.String())
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
// Verifica finale: attesa propagazione CDC->sink-vector->Qdrant, mai un
// semplice sleep fisso (stesso principio di neo4jtarget_e2e_integration_test.go).
// ============================================================

func waitForQdrantPoint(t *testing.T, ctx context.Context, client *qdrant.Client, embeddingID, wantEntityID string, timeout time.Duration) {
	t.Helper()
	pointID := derivePointIDForTest(embeddingID)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		points, err := client.Get(ctx, &qdrant.GetPoints{
			CollectionName: collectionName,
			Ids:            []*qdrant.PointId{qdrant.NewID(pointID)},
			WithPayload:    qdrant.NewWithPayload(true),
		})
		if err == nil && len(points) > 0 {
			nodeID := points[0].GetPayload()["node_id"].GetStringValue()
			if nodeID == wantEntityID {
				return
			}
			lastErr = fmt.Errorf("punto id=%s presente ma payload.node_id=%q, want %q (ancora in propagazione?)", pointID, nodeID, wantEntityID)
		} else if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("punto id=%s non ancora presente", pointID)
		}
		time.Sleep(e2ePointWaitInterval)
	}
	t.Fatalf("punto Qdrant id=%s (embedding_id=%s) con node_id=%q non comparso entro %s (propagazione CDC Postgres->Debezium->Kafka->sink-vector->Qdrant): ultimo errore/stato osservato: %v",
		pointID, embeddingID, wantEntityID, timeout, lastErr)
}

// ============================================================
// logDiagnostics stampa i log del container solo quando il test è già
// fallito (t.Failed()) — diagnostica per capire DOVE si è rotta la catena
// Postgres->Debezium->Kafka->sink-vector->Qdrant, non rumore su run verdi.
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
