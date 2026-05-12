# Lab Experience MQTT — pipeline completa

Materiale per il task **"MQTT in the Wild — versione studenti"** assegnato dal Prof. Innamorati.
Tutto è organizzato in 3 step (1 notebook + 1 script + 1 notebook), nello stesso spirito del paper di riferimento.

## Vincoli ricordati dal prof.

- **Subscribe in QoS 2** (altrimenti vediamo solo i QoS più bassi del publisher).
- **Niente `#` al root** sui broker pubblici (`test.mosquitto.org`, `broker.hivemq.com`, `broker.emqx.io`) — bisogna sottoscriversi a topic specifici o filtri tipo `<root>/#`.
- **Almeno 4 broker** da analizzare (3 noti + uno a vostra scelta).
- **~50+ topic-filter** di subscribe.
- Salvare **tutto** quello che si riceve, filtrare poi in fase di analisi.
- Catturare per il tempo necessario (≥ 1 ora per broker), idealmente ripetere più finestre.

---

## Struttura dei file

```
lab_mqtt/
├── README.md                      ← questo file
├── 01_topic_analysis.ipynb        ← step 1: analisi dei topicdf_*.pkl + export subscription_list.json
├── 02_mqtt_collector.py           ← step 2: client MQTT multi-broker, QoS 2, log a pickle.gz
├── 03_message_analysis.ipynb      ← step 3: analisi dei messaggi raccolti (figure paper-style)
└── outputs/
    ├── subscription_list.json     ← prodotto da 01, consumato da 02
    ├── top_root_topics.csv
    ├── top_level_words.csv
    ├── fig_top_roots.png
    └── fig_topic_depth_length.png
```

## Dipendenze

```bash
pip install pandas matplotlib numpy paho-mqtt jupyter
```

---

## Workflow operativo

### Step 1 — Analisi topic e generazione subscription list

```bash
jupyter notebook 01_topic_analysis.ipynb
# (oppure: jupyter nbconvert --to notebook --execute 01_topic_analysis.ipynb)
```

Cosa fa:
1. Legge i 4 pickle del prof in modo memory-safe (regex invece di `ast.literal_eval` perché alcune righe sono >80 MB).
2. Conta i top **root topic** e le top **parole-livello** in % di broker (paper-style).
3. Calcola CDF di topic depth/length.
4. **Costruisce ed esporta `outputs/subscription_list.json`** con ~60 filtri di subscribe (mix di `<root>/#`, `+/<word>`, e topic specifici di Tasmota/HomeAssistant/Zigbee2MQTT).

### Step 2 — Cattura messaggi dai broker pubblici

```bash
python 02_mqtt_collector.py \
    --subs ./outputs/subscription_list.json \
    --duration 3600 \
    --out ./outputs/captured_messages.pkl.gz
```

Caratteristiche:
- **Un thread per broker** + un thread *writer* che fa flush periodico (ogni 5000 messaggi o 30 s).
- Reconnect automatico con backoff esponenziale.
- `Ctrl-C` chiude in modo pulito (drena la queue e fa un flush finale).
- Per ogni messaggio salva: timestamp, broker, topic, depth, length, qos, retain, dup, payload_length, payload_type (json/string/numeric/bool/unknown), primi 20 byte del payload.

Per usare un set di broker custom (es. aggiungere il quarto):

```bash
cat > brokers.json <<EOF
[
    {"name": "mosquitto", "host": "test.mosquitto.org", "port": 1883},
    {"name": "hivemq",    "host": "broker.hivemq.com",  "port": 1883},
    {"name": "emqx",      "host": "broker.emqx.io",     "port": 1883},
    {"name": "flespi",    "host": "mqtt.flespi.io",     "port": 1883}
]
EOF

python 02_mqtt_collector.py \
    --subs ./outputs/subscription_list.json \
    --duration 3600 \
    --out ./outputs/captured_messages.pkl.gz \
    --brokers ./brokers.json
```

> **Suggerimento del prof.**: ripetete più finestre di 1 h in momenti diversi del giorno e poi unite i pickle (basta un piccolo script che concatena le liste). Aiuta a livellare i picchi di traffico.

### Step 3 — Analisi dei messaggi raccolti

```bash
jupyter notebook 03_message_analysis.ipynb
```

Riproduce sul **vostro** dataset le figure principali del paper:
- Distribuzione del payload type (Fig. 9)
- Distribuzione del QoS originale dei publisher
- % di messaggi *retained* per broker (Fig. 10c)
- CDF della payload length (Fig. 10a)
- CDF di topic depth e length (Fig. 8)
- Throughput per broker nel tempo
- Top topic ricevuti per broker
- Tabella riassuntiva per la presentazione

---

## Suggerita divisione del lavoro (gruppo da 3)

| Membro | Responsabilità                                                                                                  |
|-------:|-----------------------------------------------------------------------------------------------------------------|
| **A** — Topic analysis | Notebook 1: rifinire la lista di subscribe, esplorare cluster di root, scegliere quali pattern testare.    |
| **B** — Collector       | Script 2: lanciare le acquisizioni (multiple finestre), gestire i broker, monitorare i log, unire i pickle.|
| **C** — Message analysis| Notebook 3: produrre le figure finali, scrivere le slide, confrontare i risultati col paper.               |

Tutti contribuiscono alla presentazione finale (15 min): contesto MQTT/paper → metodologia → risultati → confronto col paper → limiti.

---

## Note tecniche

- **Memoria**: i pickle del prof contengono singoli broker con >500 k topic, per cui i loro `topic_list` arrivano a 80 MB di stringa. Il notebook 1 evita di parserarli con `ast.literal_eval` e usa regex mirate; non superate i ~3 GB di RAM su un laptop normale.
- **Payload type**: la classificazione segue lo stesso schema del paper. Per dettagli vedi `classify_payload()` in `02_mqtt_collector.py`.
- **Retain & DUP**: paho-mqtt espone `msg.retain` e `msg.dup`; entrambi sono nel record salvato.
- **QoS**: anche se il `subscribe` è a QoS 2, il `msg.qos` riflette il QoS scelto dal publisher, perché MQTT applica `min(pub_qos, sub_qos)` e noi siamo al massimo.
