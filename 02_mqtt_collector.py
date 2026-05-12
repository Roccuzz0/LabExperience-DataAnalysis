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

Uso:
    python 02_mqtt_collector.py \\
        --subs ./outputs/subscription_list.json \\
        --duration 3600 \\
        --out ./outputs/captured_messages.pkl.gz

Ctrl-C interrompe in modo pulito (tutti i messaggi accumulati vengono salvati).

Dipendenze:
    pip install paho-mqtt pandas
"""
from __future__ import annotations

import argparse
import gzip
import json
import pickle
import signal
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from queue import Queue, Empty
from typing import List, Dict, Any

import paho.mqtt.client as mqtt


# ---------------------------------------------------------------------------
# Default broker list (puoi sovrascrivere con --brokers)
# ---------------------------------------------------------------------------
DEFAULT_BROKERS = [
    {"name": "mosquitto", "host": "test.mosquitto.org", "port": 1883},
    {"name": "hivemq",    "host": "broker.hivemq.com",  "port": 1883},
    {"name": "emqx",      "host": "broker.emqx.io",     "port": 1883},
    # Aggiungetene almeno un quarto a piacere, es:
    # {"name": "flespi",  "host": "mqtt.flespi.io",     "port": 1883},
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
        self.client = mqtt.Client(client_id=f"polimi-labexp-{cfg.name}-{int(time.time())}",
                                  clean_session=True)
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
# Main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="Multi-broker MQTT collector (QoS 2)")
    ap.add_argument("--subs", required=True, type=Path,
                    help="JSON file con lo schema {'qos': 2, 'topics': [...]}")
    ap.add_argument("--out", required=True, type=Path,
                    help="File di output (.pkl.gz)")
    ap.add_argument("--duration", type=int, default=3600,
                    help="Durata cattura in secondi (default 3600 = 1h). 0 = all'infinito.")
    ap.add_argument("--brokers", type=Path, default=None,
                    help="JSON con lista di broker custom (default: mosquitto/hivemq/emqx)")
    args = ap.parse_args()

    sub_cfg = json.loads(args.subs.read_text())
    topics: List[str] = sub_cfg["topics"]
    qos: int = int(sub_cfg.get("qos", 2))
    print(f"Caricati {len(topics)} filtri di subscribe @ QoS {qos}")

    if args.brokers:
        broker_dicts = json.loads(args.brokers.read_text())
    else:
        broker_dicts = DEFAULT_BROKERS

    args.out.parent.mkdir(parents=True, exist_ok=True)
    queue: Queue = Queue(maxsize=200_000)
    stop_event = threading.Event()

    workers = [
        BrokerWorker(BrokerCfg(**b), topics, qos, queue, stop_event)
        for b in broker_dicts
    ]
    writer = Writer(queue, args.out, stop_event)

    # Graceful shutdown
    def _handle_sig(*_):
        print("\n>>> Interrupt ricevuto, chiudo i broker e flusho su disco...")
        stop_event.set()
    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    writer.start()
    for w in workers:
        w.start()

    deadline = (time.time() + args.duration) if args.duration > 0 else None
    try:
        while not stop_event.is_set():
            time.sleep(5)
            stats = ", ".join(f"{w.cfg.name}={w.n_received}" for w in workers)
            print(f"[main] queue={queue.qsize():>5}  ricevuti: {stats}")
            if deadline and time.time() >= deadline:
                stop_event.set()
    finally:
        stop_event.set()
        for w in workers:
            w.join(timeout=10)
        writer.join(timeout=30)

    print("\n=== Riepilogo finale ===")
    for w in workers:
        print(f"  {w.cfg.name:12s}  ricevuti = {w.n_received}")
    print(f"  Totale scritti su disco: {writer.total_written}")
    print(f"  File di output: {args.out}")


if __name__ == "__main__":
    sys.exit(main() or 0)
