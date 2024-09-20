# MATE Project
<p align="center">
    <img src="Img/Varie/MATE-logo.jpeg" width="400" style="height:auto;" />
</p>


## Descrizione

**MATE - Monitoraggio Avanzato di Temperature e Effetti** è un progetto pensato per raccogliere, elaborare e visualizzare i dati provenienti da un sensore di temperatura e umidità. Utilizza una **pipeline** che integra tecnologie come Logstash, Kafka, Spark, Elasticsearch e Kibana per gestire grandi quantità di dati storici e in tempo reale e visualizzarli su dashboard interattive. Permettendo di effettuare **Analisi climatiche** attraverso la comprensione delle variazioni climatiche nel tempo e l'identificazione di modelli stagionali.
 
Il progetto è stato realizzato utilizzando un sensore di temperatura e umidità **SwitchBot**. Tuttavia, è possibile estendere il sistema ad altri modelli di sensori modificando il file di configurazione di Logstash.

## Requisiti
Per eseguire questo progetto, è necessario:
* Sensore di temperatura e umidità **SwitchBot** (o altri sensori compatibili)
* **Chiave API** per accedere ai dati del sensore tramite SwitchBot (da specificare in Logstash)
* **Docker** e **Docker Compose** installati

## Tecnologie Utilizzate
Il progetto è composto dai seguenti componenti principali:

* **Logstash** : Per raccogliere e trasformare i dati dal sensore (SwitchBot o altri)
* **Kafka**: Per gestire lo streaming dei dati in tempo reale e storici
* **Spark**: Per arricchire ed elaborare i dati
* **Elasticsearch**: Per l'indicizzazione e la ricerca dei dati
* **Kibana**: Per la visualizzazione interattiva dei dati
* **Portainer**: Per la gestione dei container Docker

## Struttura del Repository
Il repository è così strutturato:
* `/logstash/config`: Contiene il file di configurazione per Logstash (`logstash.conf`), che gestisce la raccolta dei dati dal sensore.
* `/spark-app`: Contiene il codice per l'elaborazione dei dati in Spark, il Dockerfile e le librerie Python richieste per costruire l'immagine Spark.
* `docker-compose.yaml`: Definisce i servizi Docker necessari per avviare l'intera pipeline (Logstash, Kafka, Spark, Elasticsearch, Kibana) e altro.
* `/kibana`: Contiene il file di configurazione per Kibana, che include la dashboard per la visualizzazione dei dati raccolti.
* `/Buffer`: Contiene il file **CSV** dei dati storici del termometro `termometro_data.csv`.


## Setup e Installazione

1. **Clona il repository**
2. **API SwitchBot**
Per raccogliere i dati dal sensore SwitchBot, è necessario ottenere una [chiave API](https://github.com/OpenWonderLabs/SwitchBotAPI/blob/main/README-v1.0.md#getting-started). È importante utilizzare la versione API v1.0 e non la v1.1, poiché la sicurezza dell'autenticazione nella versione v1.1 risulta problematica per logstash. Puoi specificare la chiave API nel file `docker-compose.yaml` utilizzando la variabile `TAP_TOKEN`:
    ```yaml
    logstash:
        environment:
            TAP_TOKEN: your_api_key_here
    ```
    Se vuoi usare un'altra marca o modello di sensore, dovrai modificare il file di configurazione di Logstash (`logstash/config/logstash.conf`) per adattarlo alla nuova sorgente dati. Assicurati che i dati inviati a Kafka includano i campi:
    * `Timestamp`
    * `Temperature_Celsius`
    * `Relative_Humidity`
3. **Avvia i container**
    Ovviamente nella root del progetto esegui :
    ```bash
       docker compose up -d
    ```
    Grazie agli **health check** implementati per i vari servizi, **Docker Compose** gestirà automaticamente le dipendenze tra i container, assicurandosi che i servizi dipendenti, come `spark-app`, vengano avviati solo quando **Kafka** ed **Elasticsearch** sono completamente pronti e considerati `healthy`.
    

    Tuttavia, ti consiglio di utilizzare **Portainer** per monitorare lo stato dei container e assicurarti che tutto funzioni correttamente.

4. **Accedi a Kibana** 
    Una volta avviati tutti i container, accedi a **Kibana** tramite il browser all'indirizzo:
    ```arduino
    http://localhost:5601
    ```
    - **Credenziali di accesso** (Puoi modificare le credenziali nel file `docker-compose.yaml`.):
      * **Username**: `elastic`
      * **Password**: `1234Catania`
         
    - **Importa la dashboard**: Usa il file `Dashboard.ndjson` nella cartella `\kibana` per importare le visualizzazioni e le configurazioni.

    - **Esempio di Dashboard**: Di seguito è mostrato un esempio di come appare la dashboard importata in Kibana:

<p align="center">
    <img src="Img/Dashbord/Tre anni.png" width="700" style="height:auto;" />
</p>


## Autore
Progetto **MATE** è concesso in licenza [CC BY-NC-SA 4.0](https://creativecommons.org/licenses/by-nc-sa/4.0/?ref=chooser-v1)© di Marco Agatello 
