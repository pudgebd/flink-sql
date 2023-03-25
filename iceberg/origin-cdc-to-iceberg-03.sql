CREATE TABLE t3 (
     a string,
     b string,
     c int,
     d string,
     ts timestamp,
     PRIMARY KEY(c) NOT ENFORCED
) WITH (
     'connector' = 'mysql-cdc',
     'hostname' = 'hdfs1',
     'port' = '3306',
     'username' = 'root',
     'password' = '123456',
     'database-name' = 'demo01',
     'table-name' = 't3',
     'scan.startup.mode' = 'initial'
);

/*
CREATE TABLE t3_print (
     a string,
     b string,
     c int,
     d string,
     ts timestamp
) WITH (
     'connector' = 'print'
);

insert into t3_print select a, b, c, d, ts from t3;
*/


CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'warehouse'='hdfs://hdfs1:9000/user/hive/warehouse/',
  'uri'='thrift://hdfs2:9083',  
  'property-version'='1'
);

insert into hive_catalog.cq_cdc_07.t3 select a, b, c, d, ts from t3;
