FROM alpine:latest as downloader

# Install curl
RUN apk --no-cache add curl

# Download JAR files - using versions known to work with Trino 395
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.1/hadoop-aws-3.2.1.jar -o /hadoop-aws-3.2.1.jar \
 && curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.196/aws-java-sdk-bundle-1.12.196.jar -o /aws-java-sdk-bundle-1.12.196.jar \
 && curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.2.1/hadoop-common-3.2.1.jar -o /hadoop-common-3.2.1.jar

# Main image
FROM trinodb/trino:395

USER root

# Remove existing JAR files from relevant directories
RUN rm -f /usr/lib/trino/plugin/iceberg/hadoop-aws-*.jar \
 && rm -f /usr/lib/trino/plugin/iceberg/aws-java-sdk-bundle-*.jar \
 && rm -f /usr/lib/trino/plugin/iceberg/hadoop-common-*.jar \
 && rm -f /usr/lib/trino/lib/hadoop-aws-*.jar \
 && rm -f /usr/lib/trino/lib/aws-java-sdk-bundle-*.jar \
 && rm -f /usr/lib/trino/lib/hadoop-common-*.jar \
 && rm -f /usr/lib/trino/plugin/hive/hadoop-aws-*.jar \
 && rm -f /usr/lib/trino/plugin/hive/aws-java-sdk-bundle-*.jar \
 && rm -f /usr/lib/trino/plugin/hive/hadoop-common-*.jar \
 && rm -f /etc/hadoop/lib/hadoop-aws-*.jar \
 && rm -f /etc/hadoop/lib/aws-java-sdk-bundle-*.jar \
 && rm -f /etc/hadoop/lib/hadoop-common-*.jar

# Copy configuration files and JARs from the downloader stage
COPY catalog/kafka.properties /etc/trino/catalog/
COPY config.properties /etc/trino/
COPY node.properties /etc/trino/
COPY catalog/iceberg.properties /etc/trino/catalog/
COPY catalog/hive.properties /etc/trino/catalog/
COPY core-site.xml /etc/hadoop/conf/

# Copy JAR files to ALL plugin directories and JVM library path
COPY --from=downloader /hadoop-aws-3.2.1.jar /usr/lib/trino/plugin/iceberg/
COPY --from=downloader /aws-java-sdk-bundle-1.12.196.jar /usr/lib/trino/plugin/iceberg/
COPY --from=downloader /hadoop-common-3.2.1.jar /usr/lib/trino/plugin/iceberg/

# Copy the same JARs to other plugin directories and lib directories
RUN mkdir -p /usr/lib/trino/plugin/hive /usr/lib/trino/lib /usr/lib/trino/plugin/jmx /etc/hadoop/lib && \
    cp /usr/lib/trino/plugin/iceberg/hadoop-aws-3.2.1.jar /usr/lib/trino/lib/ && \
    cp /usr/lib/trino/plugin/iceberg/aws-java-sdk-bundle-1.12.196.jar /usr/lib/trino/lib/ && \
    cp /usr/lib/trino/plugin/iceberg/hadoop-common-3.2.1.jar /usr/lib/trino/lib/ && \
    cp /usr/lib/trino/plugin/iceberg/hadoop-aws-3.2.1.jar /usr/lib/trino/plugin/hive/ && \
    cp /usr/lib/trino/plugin/iceberg/aws-java-sdk-bundle-1.12.196.jar /usr/lib/trino/plugin/hive/ && \
    cp /usr/lib/trino/plugin/iceberg/hadoop-common-3.2.1.jar /usr/lib/trino/plugin/hive/ && \
    cp /usr/lib/trino/plugin/iceberg/hadoop-aws-3.2.1.jar /etc/hadoop/lib/ && \
    cp /usr/lib/trino/plugin/iceberg/aws-java-sdk-bundle-1.12.196.jar /etc/hadoop/lib/ && \
    cp /usr/lib/trino/plugin/iceberg/hadoop-common-3.2.1.jar /etc/hadoop/lib/

# Create necessary directories
RUN mkdir -p /data/trino/log /opt/warehouse && \
    chown -R trino:trino /data/trino /opt/warehouse /usr/lib/trino/plugin/iceberg /usr/lib/trino/plugin/hive /usr/lib/trino/lib /etc/hadoop/lib && \
    chmod -R 777 /opt/warehouse

# Switch back to trino user
USER trino

