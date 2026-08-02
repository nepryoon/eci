use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Guard ritornato da [`init_tracing`]. Deve restare vivo per tutta la
/// durata del processo (tipicamente tenuto in una variabile locale a
/// `main()`): il suo `Drop` esegue lo shutdown pulito del TracerProvider
/// OTel (flush degli span in buffer sull'exporter stdout).
pub struct TracingGuard {
    provider: SdkTracerProvider,
}

impl Drop for TracingGuard {
    fn drop(&mut self) {
        if let Err(err) = self.provider.shutdown() {
            eprintln!("eci-common: errore durante lo shutdown del TracerProvider OTel: {err}");
        }
    }
}

/// Inizializza il TracerProvider OTel con un exporter stdout/console e lo
/// collega a `tracing` tramite `tracing-opentelemetry`, così il codice
/// applicativo usa le macro standard di `tracing`
/// (`#[tracing::instrument]`, `tracing::info_span!`, ...) invece dell'API
/// OTel a basso livello direttamente.
///
/// Va chiamata una sola volta per processo. **Edge case (SPEC-010 §4)**:
/// se chiamata una seconda volta, NON panica — la seconda chiamata è un
/// no-op (il subscriber `tracing` globale già installato resta attivo),
/// con un warning esplicito su stderr. Il guard ritornato dalla chiamata
/// no-op è comunque valido (contiene un `SdkTracerProvider` proprio, mai
/// collegato al subscriber globale) e può essere droppato normalmente.
pub fn init_tracing(service_name: &str) -> TracingGuard {
    let resource = Resource::builder()
        .with_service_name(service_name.to_string())
        .build();

    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(opentelemetry_stdout::SpanExporter::default())
        .with_resource(resource)
        .build();

    let tracer = provider.tracer(service_name.to_string());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry().with(otel_layer);

    if subscriber.try_init().is_err() {
        eprintln!(
            "eci-common: init_tracing chiamata più di una volta nello stesso processo; \
             il subscriber tracing globale esistente resta attivo (no-op)."
        );
    }

    TracingGuard { provider }
}

/// Legge il trace_id (128 bit, formato esadecimale W3C Trace Context, 32
/// caratteri) dallo span attivo nel contesto corrente. `None` se non c'è
/// nessuno span attivo (o se lo span attivo non ha un contesto OTel
/// valido) — mai un panic, mai una stringa vuota silenziosa.
pub fn current_trace_id_hex() -> Option<String> {
    let span_context = tracing::Span::current().context().span().span_context().clone();
    if span_context.is_valid() {
        Some(span_context.trace_id().to_string())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{current_trace_id_hex, init_tracing};

    // init_tracing installa un subscriber tracing GLOBALE (una volta sola
    // per processo, SPEC-010 §4). Con cargo test che esegue i test in
    // thread paralleli di default, avere più #[test] che chiamano
    // init_tracing indipendentemente rischierebbe una race: il guard del
    // primo test a "vincere" l'init potrebbe droppare (shutdown del
    // TracerProvider) prima che gli altri test abbiano finito. Per questo
    // gli scenari 2/3/5 sono un solo test sequenziale: nessuna
    // concorrenza sullo stato globale condiviso, nessuna fragilità.
    #[test]
    fn scenarios_2_3_5_trace_id_lifecycle() {
        let _guard = init_tracing("eci-common-tests");

        // Scenario 3: nessuno span attivo -> None.
        assert_eq!(current_trace_id_hex(), None);

        // Scenario 2: dentro uno span attivo -> Some, 32 caratteri esadecimali.
        let outer = tracing::info_span!("scenario2_span");
        let outer_trace_id = {
            let _enter = outer.enter();
            let trace_id = current_trace_id_hex().expect("atteso Some dentro uno span attivo");
            assert_eq!(
                trace_id.len(),
                32,
                "trace_id = {trace_id:?}, attesi 32 caratteri esadecimali"
            );
            assert!(
                trace_id.chars().all(|c| c.is_ascii_hexdigit()),
                "trace_id = {trace_id:?} contiene caratteri non esadecimali"
            );
            trace_id
        };

        // Scenario 5: span annidato -> stesso trace_id dello span esterno.
        let _outer_enter = outer.enter();
        let inner = tracing::info_span!("scenario5_inner");
        let _inner_enter = inner.enter();
        let inner_trace_id = current_trace_id_hex().expect("atteso Some nello span interno");
        assert_eq!(
            outer_trace_id, inner_trace_id,
            "span annidati devono condividere lo stesso trace_id (proprietà del trace, non dello span)"
        );
    }
}
