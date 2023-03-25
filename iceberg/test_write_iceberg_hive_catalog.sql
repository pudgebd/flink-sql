CREATE TABLE datagen_source (
    id INT, 
    date_col date,
    str_col string
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '1',
 'fields.id.min' = 1,
 'fields.id.max' = 3,
 'fields.str_col.length'='1'
);


CREATE TABLE print_table(
    date_col date,
    str_col string
) WITH ('connector' = 'print');


CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'warehouse'='hdfs://hdfs1:9000/user/hive/warehouse/',
  'uri'='thrift://hdfs2:9083',  
  'property-version'='1'
);



CREATE DATABASE if not exists hive_catalog.xxx_localdebug_iceberg_db;

CREATE TABLE if not exists hive_catalog.xxx_localdebug_iceberg_db.test_field_type_01(
    date_col date,
    str_col string
) 
WITH (
    'format-version' = '2'
);

/*

CREATE TABLE t01(
     id INT NOT NULL, 
     name STRING, 
     c1 STRING, 
     primary key(id)  NOT ENFORCED
) PARTITIONED BY (name) 
WITH ( 
     'format-version' = '2', 
     'owner_name' = 'root', 
     'write.upsert.enabled' = 'true', 
     'table_type' = 'iceberg'
);

CREATE TABLE `hive_catalog`.`default`.`sample` (
    id BIGINT COMMENT 'unique id',
    data STRING
) PARTITIONED BY (data);


ALTER TABLE hive_catalog.xxx_iceberg_db.t1 SET ('write.format.default'='avro');

-- hadoop table 不能改名
ALTER TABLE hive_catalog.xxx_iceberg_db.t1 RENAME TO hive_catalog.xxx_iceberg_db.t1r1;

-- 无法识别sql
alter table hive_catalog.xxx_iceberg_db.t1 add partition (a = '2019-02-12') location 'hdfs://hdfs1:9000/user/xxx/iceberg/dev/warehouse/t1/data/a=2019-02-12';
*/

insert into hive_catalog.xxx_localdebug_iceberg_db.test_field_type_01 values (TO_DATE('2022-06-04'), 'a1');
/*

insert into hive_catalog.xxx_localdebug_iceberg_db.test_field_type_01 select * from datagen_source;
 */
 
/*
insert into print_table select * from datagen_source;
 */

