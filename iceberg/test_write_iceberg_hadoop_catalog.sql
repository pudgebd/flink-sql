CREATE TABLE datagen_source (
    a    STRING,
    b    STRING,
    c    STRING
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '50',
 'fields.a.length'='1',
 'fields.b.length'='1',
 'fields.c.length'='1'
);


CREATE TABLE print_table(
    a    STRING,
    b    STRING,
    c    STRING
) WITH ('connector' = 'print');


CREATE CATALOG hadoop_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hadoop',
  'warehouse'='hdfs://hdfs1:9000/user/xxx/iceberg/dev/warehouse/',
  'property-version'='1'
);



CREATE DATABASE if not exists hadoop_catalog.xxx_iceberg_db;

CREATE TABLE if not exists hadoop_catalog.xxx_iceberg_db.t1(
    a    STRING,
    b    STRING,
    c    STRING,
    PRIMARY KEY (b) NOT ENFORCED
) 
PARTITIONED BY (a) 
WITH (
    'format-version' = '2',
    'write.upsert.enabled' = 'true'
);

/*
ALTER TABLE hadoop_catalog.xxx_iceberg_db.t1 SET ('write.format.default'='avro');

-- hadoop table 不能改名
ALTER TABLE hadoop_catalog.xxx_iceberg_db.t1 RENAME TO hadoop_catalog.xxx_iceberg_db.t1r1;

-- 无法识别sql
alter table hadoop_catalog.xxx_iceberg_db.t1 add partition (a = '2019-02-12') location 'hdfs://hdfs1:9000/user/xxx/iceberg/dev/warehouse/t1/data/a=2019-02-12';
*/
 
 

insert into hadoop_catalog.xxx_iceberg_db.t1 select * from datagen_source;

/*
insert into print_table select * from hadoop_catalog.xxx_iceberg_db.t1;
 */

