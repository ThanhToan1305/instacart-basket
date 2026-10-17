# Keep the tested Java/Python environment of the existing prototype.
FROM apache/hadoop:3.5.0 AS hadoop_distribution

FROM instacart-basket:1.0

COPY --from=hadoop_distribution /opt/hadoop /opt/hadoop
COPY infra/hdfs-docker/node.py /opt/instacart-hdfs/node.py
COPY infra/hdfs-docker/health.py /opt/instacart-hdfs/health.py

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    HADOOP_HOME=/opt/hadoop \
    HADOOP_CONF_DIR=/etc/instacart-hdfs \
    HADOOP_HEAPSIZE_MAX=256

ENTRYPOINT ["python", "/opt/instacart-hdfs/node.py"]
CMD ["namenode"]
