FROM trinodb/trino:latest

# Copy configuration files
COPY catalog/iceberg.properties /etc/trino/catalog/
COPY config.properties /etc/trino/
COPY node.properties /etc/trino/

# Switch to root to create directories
USER root

# Create necessary directories
RUN mkdir -p /data/trino/log && \
    mkdir -p /opt/warehouse && \
    chown -R trino:trino /data/trino && \
    chown -R trino:trino /opt/warehouse && \
    chmod -R 777 /opt/warehouse

# Switch back to trino user
USER trino