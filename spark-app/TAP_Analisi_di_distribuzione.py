from kafka import KafkaConsumer
import json
import sys
import os
import time as tempo
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, floor, month, when,date_format,year,dayofmonth,udf
from elasticsearch import Elasticsearch 
from astral.sun import sun
from astral import LocationInfo

#Configurazione dei parametri del programma per renderli modificabili dall`esterno
#Parametri per ES
es_host=os.getenv("TAP_ES_HOST","127.0.0.1")
es_port=os.getenv("TAP_ES_PORT","9200")
es_user=os.getenv("TAP_ES_USER","elastic")
es_password=os.getenv("TAP_ES_PASSWORD","1234Catania")
es_index_name=os.getenv("TAP_ES_INDEX","metter_input")
#Parametri per kafka
topic=os.getenv("TAP_KAFKA_TOPIC","stream_input")
kafka_host=os.getenv("TAP_KAFKA_HOST","127.0.0.1")
kafka_port=os.getenv("TAP_KAFKA_PORT","9092")
bootstrap_servers=[f"{kafka_host}:{kafka_port}"]
max_poll_records_ENV=int(os.getenv("TAP_KAFKA_MAX_POLL_RECORDS","30000"))
fetch_max_bytes_ENV=int(os.getenv("TAP_KAFKA_FETCH_MAX_BYTES","52428800"))
fetch_max_wait_ms_ENV=int(os.getenv("TAP_KAFKA_FETCH_MAX_WAIT_MS","1000"))

#Parametri per il programma
bin_size = int(os.getenv("TAP_BIN_SIZE",1))

#Stampa dei parametri per il debug
print(f"\nDati di Elasticsearch: bootstrap[{es_host}:{es_port}] - autenticazione [{es_user}:{es_password}] - idex [{es_index_name}]")
print(f"Dati di Kafka: Topic [{topic}] - bootstrap[{kafka_host}:{kafka_port}] - max record [{max_poll_records_ENV}] - max byte [{fetch_max_bytes_ENV}] - max attesa [{fetch_max_wait_ms_ENV}]") 
print(f"Dimensione del bin: {bin_size} \n\n")

#Mapping su ES
metter_input_index = {
  "settings": {
    "index": {
      "number_of_shards":5,
      "number_of_replicas":1,
      "analysis": {
        "analyzer": {
          "custom_analyzer": {
            "type": "custom",
            "tokenizer": "standard",
            "filter": [
              "lowercase",
              "asciifolding"
            ]
          }
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "Timestamp": {
        "type": "date",
        "format": "yyyy-MM-dd HH:mm:ss"
      },
      "Temperature_Celsius": {
        "type": "float"
      },
      "Relative_Humidity": {
        "type": "float"
      },
      "temp_bin": {
        "type": "integer"
      },
      "month": {
        "type": "integer"
      },
      "day": {
        "type": "integer"
      },
      "year": {
        "type": "integer"
      },
      "season": {
        "type": "keyword"
      },
      "day_night": {
        "type": "keyword"
      }
    }
  }
}

#Metodo per verificare la connessione con Elasticsearch
def  create_elasticsearch_connection (): 
    try : 
        es = Elasticsearch(
            f"http://{es_host}:{es_port}",
            http_auth=(es_user, es_password)
        )
        # Visualizza i dettagli della connessione. 
        print( f"Connessione {es} creata correttamente" ) 
    except Exception as e: 
        #Nel caso in cui ottenessi un errore posso terminare
        print( f"Impossibile creare la connessione con ES {e}" ) 
        exit(-1)

    return es 

#Metodo per verificare se l'indice esiste già, altrimenti lo va a creare
def  check_if_index_exists ( es ): 
    if es.indices.exists(index= es_index_name ): 
        print (f"L'indice {es_index_name} esiste già" ) 
    else : 
        es.indices.create(index= es_index_name , body=metter_input_index) 
        print ( f"L'indice {es_index_name} creato" ) 

#Metodo per avviare lo streaming dei dati in Elasticsearch
def start_streaming(df, es):
    print("Lo streaming è in fase di avvio...")
    #Definizione di una variabile per il controllo del successo dell'invio dei dati
    success = False
    while not success:
        try:
            df.write.format("org.elasticsearch.spark.sql") \
                .option("es.nodes", es_host) \
                .option("es.port", es_port) \
                .option("es.net.http.auth.user", es_user) \
                .option("es.net.http.auth.pass", es_password) \
                .mode("append") \
                .save(es_index_name)
            success = True
            print("Dati inviati con successo a Elasticsearch.")
        except Exception as e:
            #Nel caso in cui desse errore riprova tra 2 secondi
            print(f"Errore durante l'invio dei dati a Elasticsearch: {e}")
            print("Riprovo tra 2 secondi...")
            tempo.sleep(2)

#Metodo per eleborare i messaggi ricevuti da Kafka
def process_messages(messages):
    rows = []
    # Itera su ogni messaggio ricevuto dal topic Kafka 
    for message in messages:
        #Imposto un try catch per gestire eventuali errori e non bloccare il programma, in caso di errore salto il messaggio. 
        #Questo potrebbe accadere quando la richiesta API di logstash non è andata a buon fine, perchè ad esempio si è superato il limite di richieste
        try:
            value = json.loads(message.value) 
            timestamp = value.get("Timestamp")
            temperature = float(value.get("Temperature_Celsius"))
            humidity = float(value.get("Relative_Humidity"))
            rows.append((timestamp, temperature, humidity))
        except (TypeError, ValueError) as e:
            # Semplicemente salta il messaggio 
            print(f"Errore nel processo del messaggio: {e}. Messaggio saltato.")
            continue
    return rows

#Metodo per aggiungere la colonna temp_bin
def add_temp_bin(df):
    return df.withColumn("temp_bin", floor(col("Temperature_Celsius") / bin_size) * bin_size)

#Metodo per aggiungere la colonna stagione, ma anche mese, giorno e anno
def add_season_column(df):
    # Estrazione mese, giorno e anno dal timestamp
    df = df.withColumn("month", month(col("Timestamp")))
    df = df.withColumn("day", dayofmonth(col("Timestamp")))
    df = df.withColumn("year", year(col("Timestamp")))
    # Inserimento della stagione in base al mese e al giorno
    df = df.withColumn("season",
        when(
            ((col("month") == 3) & (col("day") >= 21)) |
            ((col("month") > 3) & (col("month") < 6)) |
            ((col("month") == 6) & (col("day") <= 20)),
            "Primavera"
        ).when(
            ((col("month") == 6) & (col("day") >= 21)) |
            ((col("month") > 6) & (col("month") < 9)) |
            ((col("month") == 9) & (col("day") <= 20)),
            "Estate"
        ).when(
            ((col("month") == 9) & (col("day") >= 21)) |
            ((col("month") > 9) & (col("month") < 12)) |
            ((col("month") == 12) & (col("day") <= 20)),
            "Autunno"
        ).otherwise("Inverno")
    )
    return df

# Funzione per calcolare giorno o notte
def determine_day_night(timestamp_str, lat, lon):
    try:
        # Conversione del timestamp in un oggetto datetime
        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
        # Creazione di un oggetto LocationInfo per la città di Catania
        city = LocationInfo("Catania", "Italy", "Europe/Rome", lat, lon)
        # Calcolo dell'orario di alba e tramonto per la data specificata
        s = sun(city.observer, date=timestamp.date())
        # Estrazione dell'orario dal timestamp
        current_time = timestamp.time()
        # Confronta il tempo corrente con gli orari di alba e tramonto
        if s['sunrise'].time() <= current_time <= s['sunset'].time():
            return 'giorno'
        else:
            return 'notte'
    except Exception as e:
        # In caso di errore restituisce 'unknown', anche se in teoria non dovrebbe mai accadere
        print(f"Errore nella funzione per determinare notte o giorno: {e}")
        return 'unknown'

# Definisco la UDF per Spark, per ottimizzare la funzione di determinazione giorno o notte
determine_day_night_udf = udf(lambda ts: determine_day_night(ts, 37.5079, 15.0830), StringType())

# Creazione di una connessione Elasticsearch e controllo che l'indice esista   
es = create_elasticsearch_connection()
check_if_index_exists(es)


# Configurazione del consumer Kafka
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: x.decode('utf-8'),  
    max_poll_records=max_poll_records_ENV ,  
    fetch_min_bytes=1024*10 ,
    fetch_max_bytes=fetch_max_bytes_ENV ,  
    fetch_max_wait_ms=fetch_max_wait_ms_ENV ,   
    max_partition_fetch_bytes=1024*1024*1000
)

# Inizializzazione di Spark con elasticsearch connector presente nel jar scaricato alla creazione del container  
spark = SparkSession.builder \
    .appName("Spark - MATE") \
    .config("spark.jars", "/spark/jars/elasticsearch-spark-30_2.12-8.3.0.jar") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")\
    .getOrCreate()


# Definizione dello schema del DataFrame iniziale, dopo viene arricchito con altre colonne
schema = StructType([
    StructField("Timestamp", StringType(), True),
    StructField("Temperature_Celsius", DoubleType(), True),
    StructField("Relative_Humidity", DoubleType(), True)
])

# Buffer temporaneo per i messaggi
message_buffer = []

# Variabili per il conteggio dei messaggi per debug
sum_count = 0
sum_total =0

print("In attesa di messaggi da Kafka...")
# Loop infinito per l'elaborazione dei messaggi
while True:
  try:
    # Polling dei messaggi da Kafka 
      message_batch = consumer.poll(timeout_ms=1000) 
      # Verifica se ci sono messaggi nel batch
      if message_batch:
          # Elabora i messaggi ricevuti dal topic Kafka in batch per ogni partizione 
          for topic_partition, messages in message_batch.items():
              # Calcola il conteggio dei messaggi e aggiorna la somma totale per debug
              sum_count = len(messages)
              sum_total += sum_count
              # Elabora i messaggi e restituisce le righe del DataFrame
              rows = process_messages(messages)
              # Aggiungi le righe al buffer
              message_buffer.extend(rows)
              if len(message_buffer) > 0:
                  # Crea un DataFrame dal buffer in base allo schema che abbiamo definito in precedenza, per tale motivo il DataFrame inziale ha solamente 3 colonne
                  new_df = spark.createDataFrame(message_buffer, schema)
                  
                  # Aggiungiungo la colonna temp_bin e la stagione a new_df
                  new_df = add_temp_bin(new_df)
                  new_df = add_season_column(new_df)            
                      
                  #Modifico la colonna Timestamp per adattare il file CSV alle API, aggiungedo i secondi
                  new_df = new_df.withColumn("Timestamp", date_format("Timestamp", "yyyy-MM-dd HH:mm:ss"))     

                  # Applico la UDF per determinare giorno o notte della misurazione
                  new_df = new_df.withColumn("day_night", determine_day_night_udf(col("Timestamp")))    
                  # Scrivo i dati in Elasticsearch
                  start_streaming(new_df, es)

                  # Pulizia del buffer
                  message_buffer = []
                  
                  print(f"Somma totale dei conteggi: { sum_count } e { sum_total }")
  except KeyboardInterrupt:
      print("Interruzione manuale ricevuta, fermo Spark...")
      spark.stop()
      print("Spark fermato. Uscita.")
      sys.exit(0)


