#\!/bin/sh

# Replace the core-site.xml with properly formatted tags
cat > /etc/hadoop/conf/core-site.xml << 'EOF'
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
EOF

# Ensure permissions are correct
chown -R trino:trino /etc/hadoop/conf/core-site.xml
chmod 644 /etc/hadoop/conf/core-site.xml

echo "Trino initialization complete - core-site.xml has been configured correctly."
