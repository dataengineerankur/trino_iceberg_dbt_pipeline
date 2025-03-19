#\!/bin/bash

# Copy the corrected core-site.xml to the container
docker cp /Users/ankurchopra/repo_projects/projects/trino_iceberg_dbt_pipeline/trino/core-site.xml trino:/etc/hadoop/conf/core-site.xml

# Set proper ownership and permissions
docker exec trino chmod 644 /etc/hadoop/conf/core-site.xml
docker exec trino chown trino:trino /etc/hadoop/conf/core-site.xml

# Restart Trino to pick up the changes
docker restart trino

echo "Updated core-site.xml in the Trino container and restarted Trino"
