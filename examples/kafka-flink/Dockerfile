# syntax=docker/dockerfile:1
FROM flink:1.20.2-scala_2.12-java17
ADD --checksum=sha256:1086f3eee73d727e234860fcd03adafc0d76f2fc70a25d39c36427693fff749d https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar /opt/flink/lib/flink-sql-connector-kafka.jar
USER root
RUN chmod 644 /opt/flink/lib/flink-sql-connector-kafka.jar
USER flink
