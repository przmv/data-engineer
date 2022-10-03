FROM docker.io/apache/spark-py
COPY . .
ENTRYPOINT ["/opt/spark/bin/spark-submit", "wordcount.py"]
