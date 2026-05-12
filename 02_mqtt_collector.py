#!/usr/bin/env python3
"""
Lab MQTT — 02. Multi-broker MQTT collector
===========================================

Si connette in parallelo a più broker MQTT pubblici, si sottoscrive (con QoS 2,
come richiesto dal prof.) alla lista di topic prodotta da `01_topic_analysis.ipynb`,
e logga ogni messaggio ricevuto su un file pickle compresso (gzip), nello
stesso schema usato nel paper "MQTT in the Wild":

    {
        "timestamp"      : float (epoch seconds),
        "broker"         : str,
        "topic"          : str,
        "topic_depth"    : int,   # numero di livelli
        "topic_length"   : int,   # bytes
        "qos"            : int    (0/1/2 — QoS originale del publisher),
        "retain"         : bool,
        "dup"            : bool,
        "payload_length" : int,
        "payload_type"   : str    ("json" | "string" | "numeric" | "bool" | "unknown"),
        "payload_head"   : bytes  (primi 20 byte del payload, per debug/decoding successivo),
    }

Uso (CLI / locale):
    python 02_mqtt_collector.py \\
        --subs ./outputs/subscription_list.json \\
        --duration 3600 \\
        --out ./outputs/captured_messages.pkl.gz

Uso (Kaggle notebook):
    # 1) Settings -> Internet: ON (necessario per i broker pubblici).
    # 2) Carica come Kaggle dataset la cartella che contiene questo file +
    #    subscription_list.json (oppure incollali nel working dir).
    # 3) In una cella:
    !pip -q install paho-mqtt

    # 4) Il nome "02_mqtt_collector" inizia con una cifra e non e' un identifier
    #    Python valido: carichiamo il file come modulo via importlib.util.
    import importlib.util, sys
    spec = importlib.util.spec_from_file_location(
        "mqtt_collector",
        "/kaggle/input/<your-dataset>/02_mqtt_collector.py",
    )
    mqtt_collector = importlib.util.module_from_spec(spec)
    sys.modules["mqtt_collector"] = mqtt_collector
    spec.loader.exec_module(mqtt_collector)

    # 5) Avvia la cattura (l'output di /kaggle/working/ e' scaricabile a fine run).
    mqtt_collector.run(
        subs_path="/kaggle/input/<your-dataset>/subscription_list.json",
        out_path="/kaggle/working/captured_messages.pkl.gz",
        duration=3600,
    )

    # Alternativa "quick & dirty": esegui lo script come processo separato.
    #   !python /kaggle/input/<your-dataset>/02_mqtt_collector.py \
    #       --subs /kaggle/input/<your-dataset>/subscription_list.json \
    #       --out  /kaggle/working/captured_messages.pkl.gz \
    #       --duration 3600

Ctrl-C interrompe in modo pulito (tutti i messaggi accumulati vengono salvati).

Dipendenze:
    pip install paho-mqtt pandas
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import pickle
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Queue, Empty
from typing import List, Dict, Any, Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    # Auto-install su ambienti puliti tipo Kaggle / Colab: il modulo
    # non e' preinstallato, ma abbiamo pip a disposizione.
    import subprocess
    print("[bootstrap] paho-mqtt non trovato, lo installo con pip...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "--quiet", "paho-mqtt"]
    )
    import paho.mqtt.client as mqtt  # noqa: E402


# ---------------------------------------------------------------------------
# Kaggle detection + path defaults
# ---------------------------------------------------------------------------
IS_KAGGLE = bool(
    os.environ.get("KAGGLE_KERNEL_RUN_TYPE")
    or os.environ.get("KAGGLE_URL_BASE")
    or Path("/kaggle/working").exists()
)

if IS_KAGGLE:
    # In Kaggle gli input vengono montati sotto /kaggle/input/<dataset-slug>/.
    # Il nome del dataset non e' noto a priori: usiamo il primo subs file
    # trovato sotto /kaggle/input, oppure un placeholder che l'utente puo'
    # sovrascrivere passando subs_path a run().
    def _autodetect_kaggle_subs() -> Path:
        root = Path("/kaggle/input")
        if root.exists():
            for p in root.glob("*/subscription_list.json"):
                return p
            for p in root.rglob("subscription_list.json"):
                return p
        return root / "subscription_list.json"

    DEFAULT_SUBS_PATH = _autodetect_kaggle_subs()
    DEFAULT_OUT_PATH = Path("/kaggle/working/captured_messages.pkl.gz")
else:
    DEFAULT_SUBS_PATH = Path("./outputs/subscription_list.json")
    DEFAULT_OUT_PATH = Path("./outputs/captured_messages.pkl.gz")


# ---------------------------------------------------------------------------
# paho-mqtt v1/v2 compatibility shim
# ---------------------------------------------------------------------------
# paho-mqtt 2.x richiede di specificare callback_api_version. Le callback
# definite in questo file usano la firma legacy (V1), che e' supportata
# da entrambe le versioni a patto di richiederla esplicitamente in 2.x.
def _make_mqtt_client(client_id: str) -> mqtt.Client:
    try:
        api_v1 = mqtt.CallbackAPIVersion.VERSION1  # paho 2.x
        return mqtt.Client(
            callback_api_version=api_v1,
            client_id=client_id,
            clean_session=True,
        )
    except AttributeError:
        # paho 1.x: nessun callback_api_version
        return mqtt.Client(client_id=client_id, clean_session=True)


# ---------------------------------------------------------------------------
# Default broker list (puoi sovrascrivere con --brokers)
# ---------------------------------------------------------------------------
DEFAULT_BROKERS = [
    {"name": "mosquitto", "host": "test.mosquitto.org", "port": 1883},
    {"name": "hivemq",    "host": "broker.hivemq.com",  "port": 1883},
    {"name": "emqx",      "host": "broker.emqx.io",     "port": 1883},
    {"name": "flespi",  "host": "mqtt.flespi.io",     "port": 1883},
]


# ---------------------------------------------------------------------------
# Payload classification (stesso schema del paper)
# ---------------------------------------------------------------------------
def classify_payload(payload: bytes) -> str:
    """Restituisce 'json' | 'string' | 'numeric' | 'bool' | 'unknown'."""
    if not payload:
        return "unknown"
    # Prova decoding UTF-8 (lo MQTT non lo richiede ma è quasi sempre così)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        return "unknown"
    text_strip = text.strip()
    # bool
    if text_strip.lower() in ("true", "false", "0", "1"):
        # se è solo 0/1 lo tengo come numerico, ma "true"/"false" come bool
        if text_strip.lower() in ("true", "false"):
            return "bool"
    # numeric
    try:
        float(text_strip)
        return "numeric"
    except ValueError:
        pass
    # json
    if text_strip and text_strip[0] in "{[":
        try:
            json.loads(text_strip)
            return "json"
        except json.JSONDecodeError:
            pass
    # fallback: stringa stampabile
    if all(32 <= b < 127 or b in (9, 10, 13) for b in payload):
        return "string"
    return "unknown"


# ---------------------------------------------------------------------------
# Per-broker worker
# ---------------------------------------------------------------------------
@dataclass
class BrokerCfg:
    name: str
    host: str
    port: int = 1883
    keepalive: int = 60


class BrokerWorker(threading.Thread):
    """Un thread per broker. Mette ogni messaggio ricevuto nella queue condivisa."""

    def __init__(self, cfg: BrokerCfg, subs: List[str], qos: int, queue: Queue,
                 stop_event: threading.Event):
        super().__init__(daemon=True, name=f"broker-{cfg.name}")
        self.cfg = cfg
        self.subs = subs
        self.qos = qos
        self.queue = queue
        self.stop_event = stop_event
        self.n_received = 0
        self.client = _make_mqtt_client(
            client_id=f"polimi-labexp-{cfg.name}-{int(time.time())}"
        )
        self.client.on_connect    = self._on_connect
        self.client.on_message    = self._on_message
        self.client.on_disconnect = self._on_disconnect

    # --- callbacks -----------------------------------------------------------
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[{self.cfg.name}] CONNECT OK")
            # Subscribe in batch
            sub_list = [(t, self.qos) for t in self.subs]
            # paho accetta liste di max ~ qualche centinaio per chiamata
            CHUNK = 100
            for i in range(0, len(sub_list), CHUNK):
                client.subscribe(sub_list[i:i+CHUNK])
            print(f"[{self.cfg.name}] subscribed to {len(self.subs)} filters @ QoS {self.qos}")
        else:
            print(f"[{self.cfg.name}] CONNECT FAILED, rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print(f"[{self.cfg.name}] disconnessione inattesa (rc={rc}), tenterà reconnect")

    def _on_message(self, client, userdata, msg):
        try:
            payload = bytes(msg.payload)
            record = {
                "timestamp"     : time.time(),
                "broker"        : self.cfg.name,
                "topic"         : msg.topic,
                "topic_depth"   : sum(1 for p in msg.topic.split('/') if p),
                "topic_length"  : len(msg.topic.encode("utf-8")),
                "qos"           : msg.qos,
                "retain"        : bool(msg.retain),
                "dup"           : bool(msg.dup),
                "payload_length": len(payload),
                "payload_type"  : classify_payload(payload),
                "payload_head"  : payload[:20],
            }
            self.queue.put(record)
            self.n_received += 1
        except Exception as e:
            print(f"[{self.cfg.name}] errore on_message: {e}")

    # --- run loop ------------------------------------------------------------
    def run(self):
        backoff = 2
        while not self.stop_event.is_set():
            try:
                self.client.connect(self.cfg.host, self.cfg.port, self.cfg.keepalive)
                # loop_start lancia thread interno; noi attendiamo fino a stop
                self.client.loop_start()
                while not self.stop_event.is_set():
                    self.stop_event.wait(timeout=1.0)
                self.client.loop_stop()
                try:
                    self.client.disconnect()
                except Exception:
                    pass
                break
            except Exception as e:
                print(f"[{self.cfg.name}] connection error: {e}, retry in {backoff}s")
                if self.stop_event.wait(timeout=backoff):
                    break
                backoff = min(backoff * 2, 60)


# ---------------------------------------------------------------------------
# Writer thread: scrive periodicamente su disco
# ---------------------------------------------------------------------------
class Writer(threading.Thread):
    def __init__(self, queue: Queue, out_path: Path, stop_event: threading.Event,
                 flush_every: int = 5000, flush_seconds: float = 30.0):
        super().__init__(daemon=True, name="writer")
        self.queue = queue
        self.out_path = out_path
        self.stop_event = stop_event
        self.flush_every = flush_every
        self.flush_seconds = flush_seconds
        self.buffer: List[Dict[str, Any]] = []
        self.total_written = 0
        self.lock = threading.Lock()

    def _flush(self):
        if not self.buffer:
            return
        # Append-style: leggiamo l'esistente, concateniamo, riscriviamo.
        # Per dataset grandi conviene usare un formato a record (jsonl/parquet),
        # ma per durata 1-4h va benissimo.
        existing: List[Dict[str, Any]] = []
        if self.out_path.exists():
            try:
                with gzip.open(self.out_path, "rb") as fh:
                    existing = pickle.load(fh)
            except Exception as e:
                print(f"[writer] WARN: impossibile rileggere {self.out_path}: {e}")
        existing.extend(self.buffer)
        tmp = self.out_path.with_suffix(self.out_path.suffix + ".tmp")
        with gzip.open(tmp, "wb") as fh:
            pickle.dump(existing, fh, protocol=pickle.HIGHEST_PROTOCOL)
        tmp.replace(self.out_path)
        self.total_written += len(self.buffer)
        print(f"[writer] flush: +{len(self.buffer)} messaggi (totale su disco: {self.total_written})")
        self.buffer.clear()

    def run(self):
        last_flush = time.time()
        while not self.stop_event.is_set():
            try:
                rec = self.queue.get(timeout=1.0)
                self.buffer.append(rec)
            except Empty:
                pass
            now = time.time()
            if (len(self.buffer) >= self.flush_every or
                (self.buffer and now - last_flush >= self.flush_seconds)):
                with self.lock:
                    self._flush()
                last_flush = now
        # Drain finale
        while True:
            try:
                self.buffer.append(self.queue.get_nowait())
            except Empty:
                break
        with self.lock:
            self._flush()


# ---------------------------------------------------------------------------
# Core run() — usabile sia da CLI che da notebook (Kaggle / Jupyter)
# ---------------------------------------------------------------------------
def run(
    subs_path: Optional[os.PathLike] = None,
    out_path: Optional[os.PathLike] = None,
    duration: int = 3600,
    brokers: Optional[List[Dict[str, Any]]] = None,
    brokers_path: Optional[os.PathLike] = None,
) -> Dict[str, Any]:
    """Avvia la cattura multi-broker.

    Args:
        subs_path:    JSON con schema {"qos": int, "topics": [...]}. Default: DEFAULT_SUBS_PATH.
        out_path:     File .pkl.gz di output. Default: DEFAULT_OUT_PATH.
        duration:     Secondi di cattura (0 = all'infinito).
        brokers:      Lista di dict {"name","host","port"} (override diretto).
        brokers_path: JSON con lista broker custom (alternativa a `brokers`).

    Returns:
        Dict con statistiche finali (utile per notebook Kaggle).
    """
    subs_path = Path(subs_path) if subs_path else DEFAULT_SUBS_PATH
    out_path = Path(out_path) if out_path else DEFAULT_OUT_PATH

    if not subs_path.exists():
        raise FileNotFoundError(
            f"subs_path non trovato: {subs_path}. "
            f"Su Kaggle, carica subscription_list.json come dataset "
            f"e passa il path completo a run(subs_path=...)."
        )

    sub_cfg = json.loads(subs_path.read_text())
    topics: List[str] = sub_cfg["topics"]
    qos: int = int(sub_cfg.get("qos", 2))
    print(f"Caricati {len(topics)} filtri di subscribe @ QoS {qos} da {subs_path}")

    if brokers is not None:
        broker_dicts = brokers
    elif brokers_path is not None:
        broker_dicts = json.loads(Path(brokers_path).read_text())
    else:
        broker_dicts = DEFAULT_BROKERS

    out_path.parent.mkdir(parents=True, exist_ok=True)
    queue: Queue = Queue(maxsize=200_000)
    stop_event = threading.Event()

    workers = [
        BrokerWorker(BrokerCfg(**b), topics, qos, queue, stop_event)
        for b in broker_dicts
    ]
    writer = Writer(queue, out_path, stop_event)

    # Graceful shutdown — signal.signal funziona solo nel main thread del
    # processo. In Kaggle/Jupyter la cella gira nel main thread del kernel,
    # quindi di norma OK; ma se siamo in un thread secondario evitiamo che
    # un ValueError abbatta l'avvio.
    def _handle_sig(*_):
        print("\n>>> Interrupt ricevuto, chiudo i broker e flusho su disco...")
        stop_event.set()

    try:
        signal.signal(signal.SIGINT, _handle_sig)
        signal.signal(signal.SIGTERM, _handle_sig)
    except (ValueError, OSError) as e:
        print(f"[main] signal handler non registrato ({e}); "
              f"usa stop_event manualmente se necessario.")

    writer.start()
    for w in workers:
        w.start()

    deadline = (time.time() + duration) if duration > 0 else None
    try:
        while not stop_event.is_set():
            time.sleep(5)
            stats = ", ".join(f"{w.cfg.name}={w.n_received}" for w in workers)
            print(f"[main] queue={queue.qsize():>5}  ricevuti: {stats}")
            if deadline and time.time() >= deadline:
                stop_event.set()
    except KeyboardInterrupt:
        # Su Kaggle/Jupyter l'utente preme "interrupt kernel": catturiamo qui
        # per evitare uno stack trace e procediamo con lo shutdown ordinato.
        print("\n>>> KeyboardInterrupt: avvio shutdown ordinato...")
        stop_event.set()
    finally:
        stop_event.set()
        for w in workers:
            w.join(timeout=10)
        writer.join(timeout=30)

    print("\n=== Riepilogo finale ===")
    per_broker = {}
    for w in workers:
        print(f"  {w.cfg.name:12s}  ricevuti = {w.n_received}")
        per_broker[w.cfg.name] = w.n_received
    print(f"  Totale scritti su disco: {writer.total_written}")
    print(f"  File di output: {out_path}")

    return {
        "out_path": str(out_path),
        "total_written": writer.total_written,
        "per_broker_received": per_broker,
    }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Multi-broker MQTT collector (QoS 2)")
    ap.add_argument("--subs", type=Path, default=DEFAULT_SUBS_PATH,
                    help=f"JSON file con lo schema {{'qos': 2, 'topics': [...]}} "
                         f"(default: {DEFAULT_SUBS_PATH})")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT_PATH,
                    help=f"File di output (.pkl.gz) (default: {DEFAULT_OUT_PATH})")
    ap.add_argument("--duration", type=int, default=3600,
                    help="Durata cattura in secondi (default 3600 = 1h). 0 = all'infinito.")
    ap.add_argument("--brokers", type=Path, default=None,
                    help="JSON con lista di broker custom (default: mosquitto/hivemq/emqx)")
    args = ap.parse_args()

    run(
        subs_path=args.subs,
        out_path=args.out,
        duration=args.duration,
        brokers_path=args.brokers,
    )


def _in_jupyter_kernel() -> bool:
    """True se siamo dentro un kernel Jupyter/Colab/Kaggle (e quindi sys.argv
    NON contiene i nostri argomenti CLI, ma quelli del kernel launcher)."""
    if "ipykernel" in sys.modules or "google.colab" in sys.modules:
        return True
    argv0 = (sys.argv[0] or "").lower()
    if "ipykernel" in argv0 or "colab_kernel_launcher" in argv0:
        return True
    # tipico: -f /path/kernel-xxx.json fra gli argomenti
    if any(a == "-f" for a in sys.argv[1:]):
        return True
    return False


if __name__ == "__main__":
    if _in_jupyter_kernel():
        if IS_KAGGLE:
            # Save & Commit di Kaggle esegue il .py top-to-bottom come notebook:
            # qui vogliamo lanciare la cattura direttamente, usando i default
            # Kaggle-aware (subscription_list.json autodetected sotto
            # /kaggle/input/*/, output in /kaggle/working/).
            # La durata e' override-abile via env var MQTT_DURATION.
            _dur = int(os.environ.get("MQTT_DURATION", "3600"))
            print(f"[main] Kaggle kernel rilevato: avvio run() (duration={_dur}s).")
            print(f"       subs_path = {DEFAULT_SUBS_PATH}")
            print(f"       out_path  = {DEFAULT_OUT_PATH}")
            run(duration=_dur)
        else:
            # %run da Jupyter locale / Colab: di solito l'utente vuole solo
            # caricare le funzioni e poi chiamare run(...) in una cella separata.
            print("[main] kernel Jupyter/Colab locale rilevato: salto argparse.")
            print("       Usa run(subs_path=..., out_path=..., duration=...).")
    else:
        sys.exit(main() or 0)
