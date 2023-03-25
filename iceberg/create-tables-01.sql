CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'warehouse'='hdfs://hdfs1:9000/user/hive/warehouse/',
  'uri'='thrift://hdfs2:9083',  
  'property-version'='1'
);

CREATE TABLE `hive_catalog`.`cq_cdc_07`.`brand` (
     id INT NOT NULL, 
     brand_no STRING NOT NULL, 
     name STRING NOT NULL, 
     en_name STRING, 
     en_short_name STRING, 
     opcode STRING, 
     category STRING NOT NULL, 
     belonger STRING NOT NULL, 
     status STRING, 
     sys_no STRING NOT NULL, 
     search_code STRING, 
     parent_brand_id INT, 
     logo_url STRING, 
     create_user STRING, 
     create_time STRING NOT NULL, 
     update_user STRING, 
     update_time STRING, 
     remark STRING, 
     time_seq BIGINT, 
     organ_type_no STRING, 
     primary key(id)  NOT ENFORCED
) WITH ( 
     'format-version' = '2', 
     'write.upsert.enabled' = 'true', 
     'table_type' = 'iceberg'
);

/*
CREATE TABLE `hive_catalog`.`cq_cdc_07`.`my_ka_09`(
     id INT NOT NULL, 
     name STRING, 
     dt DATE, 
     dtime TIMESTAMP_LTZ(6), 
     `timestamp` TIMESTAMP(6), 
     dt_ori TIMESTAMP_LTZ(6), 
     ts_ori TIMESTAMP(6), 
     ti STRING, 
     sli STRING, 
     bgi BIGINT, 
     fl FLOAT, 
     ti_ori INT, 
     smli_ori INT, 
     primary key(id)  NOT ENFORCED
) WITH ( 
     'format-version' = '2', 
     'owner_name' = 'mjtest', 
     'write.upsert.enabled' = 'true', 
     'table_type' = 'iceberg'
    );
*/

/*
CREATE TABLE `hive_catalog`.`cq_cdc_07`.`test_507` (
`id` INT NOT NULL,
`name` string,
`sex` string,
`birthday` DATE,
`job` string,
`birthday1` TIMESTAMP(6),
`birthday2` TIMESTAMP(6) WITH LOCAL TIME ZONE,
`datetime_p1` TIMESTAMP(1) WITH LOCAL TIME ZONE,
`datetime_p3` TIMESTAMP(3) WITH LOCAL TIME ZONE,
`datetime_p4` TIMESTAMP(4) WITH LOCAL TIME ZONE,
`datetime_p6` TIMESTAMP(6) WITH LOCAL TIME ZONE,
 PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
'format-version' = '2',
'write.upsert.enabled' = 'true',
'table_type' = 'iceberg'
);

CREATE TABLE `hive_catalog`.`cq_cdc_07`.`test_507` (
`id` INT NOT NULL,
`name` string,
`sex` string,
`birthday` DATE,
`job` string,
`birthday1` string,
`birthday2` string,
`datetime_p1` string,
`datetime_p3` string,
`datetime_p4` string,
`datetime_p6` string,
`ts_p3` TIMESTAMP(3),
 PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
'format-version' = '2',
'write.upsert.enabled' = 'true',
'table_type' = 'iceberg'
);
*/