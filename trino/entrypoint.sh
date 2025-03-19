#!/bin/bash

# Create proper core-site.xml with name tags instead of n tags
cat > /etc/hadoop/conf/core-site.xml << 'XML'
<configuration>
    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3n.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>
</configuration>
XML

# Add AWS SDK for Java version to Java System Properties
# This ensures Trino uses our version consistently
export JAVA_OPTS="$JAVA_OPTS -Daws.sdk.version=1.12.196"

# Ensure proper permissions
chown -R trino:trino /etc/hadoop/conf/core-site.xml
chmod 644 /etc/hadoop/conf/core-site.xml

# Start Trino
exec /usr/lib/trino/bin/run-trino