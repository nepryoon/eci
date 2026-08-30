package embedclient

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

// stubServer avvia un server TCP minimale (un solo accept, poi chiude) che
// risponde SEMPRE con rawResponse grezzo, indipendentemente dalla richiesta
// ricevuta — sufficiente per esercitare i percorsi di errore HTTP del
// client (status non-200, corpo malformato) senza bisogno del vero
// embedder-fake (che risponde correttamente per costruzione). Non è un
// mock del client: è un vero server TCP/HTTP, il client gli parla davvero
// via rete — stesso principio di services/ingestion/src/embedding.rs
// (SPEC-023), replicato qui.
func stubServer(t *testing.T, rawResponse string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		// Drena la richiesta fino alla riga vuota che separa header/corpo
		// (nessun bisogno di leggere/validare il corpo qui: rawResponse è
		// fisso indipendentemente dalla richiesta).
		for {
			line, err := reader.ReadString('\n')
			if err != nil || line == "\r\n" || line == "\n" {
				break
			}
		}
		_, _ = conn.Write([]byte(rawResponse))
	}()

	return "http://" + listener.Addr().String()
}

// --- SPEC-030 §2, stesso comportamento di SPEC-023 §3 scenario 5: client
// puntato su un indirizzo che non risponde -> errore esplicito, non un
// panic né un vettore vuoto silenzioso. 127.0.0.1:1 (porta privilegiata,
// nessun listener possibile senza root). ---
func TestEmbedUnreachableAddressReturnsErrNotPanic(t *testing.T) {
	client := New("http://127.0.0.1:1")
	_, err := client.Embed(context.Background(), "qualunque testo")
	if err == nil {
		t.Fatal("atteso errore su indirizzo irraggiungibile, ottenuto nil")
	}
	if !IsUnavailable(err) {
		t.Fatalf("transport error must be classified unavailable: %v", err)
	}
}

// --- edge case: risposta HTTP con status diverso da 200 -> errore
// esplicito col codice di stato incluso nel messaggio. ---
func TestEmbedNon200StatusReturnsErrWithStatusCodeInMessage(t *testing.T) {
	baseURL := stubServer(t, "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"boom\"}")
	client := New(baseURL)
	_, err := client.Embed(context.Background(), "testo qualunque")
	if err == nil {
		t.Fatal("atteso errore su status 500")
	}
	if got := err.Error(); !strings.Contains(got, "500") {
		t.Fatalf("il messaggio d'errore deve includere il codice di stato: %s", got)
	}
	if !IsUnavailable(err) {
		t.Fatalf("HTTP 500 must be classified unavailable: %v", err)
	}
}

func TestEmbedClientErrorIsRetryableApplicationFailure(t *testing.T) {
	baseURL := stubServer(t, "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"error\":\"invalid input\"}")
	_, err := New(baseURL).Embed(context.Background(), "testo qualunque")
	if err == nil {
		t.Fatal("atteso errore su status 400")
	}
	if IsUnavailable(err) {
		t.Fatalf("HTTP 400 is an application failure, not dependency unavailability: %v", err)
	}
}

// --- edge case: risposta 200 ma JSON malformato/non un array di vettori
// -> errore esplicito di deserializzazione, non un panic. ---
func TestEmbed200WithMalformedJSONReturnsErrNotPanic(t *testing.T) {
	baseURL := stubServer(t, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"not\":\"a vector\"}")
	client := New(baseURL)
	_, err := client.Embed(context.Background(), "testo qualunque")
	if err == nil {
		t.Fatal("atteso errore su corpo 200 non-vettore")
	}
}

// --- edge case: risposta 200 con corpo non JSON valido affatto -> stesso
// trattamento, errore di deserializzazione, non panic. ---
func TestEmbed200WithInvalidJSONSyntaxReturnsErrNotPanic(t *testing.T) {
	baseURL := stubServer(t, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\nnon e' json affatto")
	client := New(baseURL)
	_, err := client.Embed(context.Background(), "testo qualunque")
	if err == nil {
		t.Fatal("atteso errore su corpo 200 non-JSON")
	}
}

// --- edge case: context già scaduto -> errore esplicito, non un blocco. ---
func TestEmbedCanceledContextReturnsErr(t *testing.T) {
	client := New("http://127.0.0.1:9")
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)
	_, err := client.Embed(ctx, "testo")
	if err == nil {
		t.Fatal("atteso errore su context scaduto")
	}
}
