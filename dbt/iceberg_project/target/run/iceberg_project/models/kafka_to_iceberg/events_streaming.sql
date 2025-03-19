-- back compat for old kwarg name
  
  
        
            
            
        

        

        merge into "iceberg"."default"."events_streaming" as DBT_INTERNAL_DEST
            using "iceberg"."default"."events_streaming__dbt_tmp" as DBT_INTERNAL_SOURCE
            on (
                DBT_INTERNAL_SOURCE.id = DBT_INTERNAL_DEST.id
            )

        
        when matched then update set
            "id" = DBT_INTERNAL_SOURCE."id","name" = DBT_INTERNAL_SOURCE."name","created_at" = DBT_INTERNAL_SOURCE."created_at","_partition_id" = DBT_INTERNAL_SOURCE."_partition_id","_partition_offset" = DBT_INTERNAL_SOURCE."_partition_offset","processed_at" = DBT_INTERNAL_SOURCE."processed_at"
        

        when not matched then insert
            ("id", "name", "created_at", "_partition_id", "_partition_offset", "processed_at")
        values
            (DBT_INTERNAL_SOURCE."id", DBT_INTERNAL_SOURCE."name", DBT_INTERNAL_SOURCE."created_at", DBT_INTERNAL_SOURCE."_partition_id", DBT_INTERNAL_SOURCE."_partition_offset", DBT_INTERNAL_SOURCE."processed_at")

    
