# Usa un'immagine di OpenJDK 11 slim come base 
FROM openjdk:11-slim

# Installa Python 3, pip, curl e procps (per includere il comando 'ps' che richede Spark)
RUN apt-get update && apt-get install -y python3 python3-pip curl procps && rm -rf /var/lib/apt/lists/*

# Imposta la directory di lavoro nel container
WORKDIR /app

# Scarica elasticsearch-spark-connector per evitare di scaricarlo ogni volta 
RUN mkdir -p /spark/jars && \
    curl -o /spark/jars/elasticsearch-spark-30_2.12-8.3.0.jar \
    https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.3.0/elasticsearch-spark-30_2.12-8.3.0.jar


# Copia il file requirements.txt nella directory di lavoro
COPY requirements.txt .

# Installa le dipendenze Python specificate nel file requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copia tutti i file dell'applicazione nella directory di lavoro
COPY . .

# Comando per eseguire l'applicazione, con l'opzione -u per evitare il buffering dell'output ed ottenere i log in tempo reale
CMD ["python3","-u", "TAP_Analisi_di_distribuzione.py"]