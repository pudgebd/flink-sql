CREATE TABLE if not exists mf_mapping.dlink_default.cq_dim_mysql (
     id bigint,
     channel string,
     name STRING,
     score double,
     PRIMARY KEY(id) NOT ENFORCED
) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = '10.201.0.82:',
     'port' = '3306',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'cdc',
     'table-name' = 'cq_dim_mysql',
     'scan.startup.mode' = 'latest-offset'
);
     
CREATE TABLE IF NOT EXISTS linkhouse.mf_cq.upsert_no_primary_key(
     id bigint,
     channel string,
     name STRING,
     score double
) WITH ( 
    'format-version' = '2', 
    'write.upsert.enabled' = 'true',
    'table_type' = 'iceberg'
);

insert into linkhouse.mf_cq.upsert_no_primary_key select * from mf_mapping.dlink_default.cq_dim_mysql;