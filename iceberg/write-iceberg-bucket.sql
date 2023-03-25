CREATE TABLE datagen_source (
    id INT, 
    name string,
    parti_col string,
    bucket_col_01 string,
    bucket_col_02 string
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '100',
 'fields.name.length'='10',
 'fields.parti_col.length'='1',
 'fields.bucket_col_01.length'='10',
 'fields.bucket_col_02.length'='10'
);


CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'warehouse'='hdfs://hdfs1:9000/user/hive/warehouse/',
  'uri'='thrift://hdfs2:9083',  
  'property-version'='1'
);


/*
CREATE DATABASE if not exists hive_catalog.xxx_localdebug_iceberg_db;


CREATE TABLE if not exists hive_catalog.xxx_localdebug_iceberg_db.write_bucket(
    id INT,
    name string,
    parti_col string,
    bucket_col_01 string,
    primary key (id)  NOT ENFORCED
) PARTITIONED BY (parti_col) 
WITH ( 
    'format-version' = '2', 
    'write.upsert.enabled' = 'true',
    'table_type' = 'iceberg'
);
*/

/**/
insert into hive_catalog.xxx_localdebug_iceberg_db.write_bucket select * from datagen_source;
