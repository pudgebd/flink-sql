CREATE TABLE if not exists cdc_cn_str (
     a string,
     b string,
     c string,
     d string,
     ts timestamp,
     PRIMARY KEY(c) NOT ENFORCED
) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = '10.201.0.82',
     'port' = '3306',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'cdc',
     'table-name' = 'cn_str',
     'scan.startup.mode' = 'initial'
);
     
CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'warehouse'='hdfs://hdfs1:9000/user/hive/warehouse/',
  'uri'='thrift://hdfs2:9083',  
  'property-version'='1'
);

insert into hive_catalog.xxx_localdebug_iceberg_db.cn_str select * from cdc_cn_str;