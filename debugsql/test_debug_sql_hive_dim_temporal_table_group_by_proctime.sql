CREATE CATALOG myhive WITH ('be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default');


SET table.sql-dialect=default;
CREATE TABLE default_catalog.default_database.MyKafkaSrc01(
    channel STRING,
    goods_id INT,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'MyKafkaSrc01',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);


CREATE TABLE print_table_1 (
    channel STRING,
    goods_id INT,
    name STRING,
    dt STRING
) WITH ('connector' = 'print');


CREATE TABLE output_table (
    channel STRING,
    goods_id INT,
    name STRING,
    dt STRING
) WITH (
 'connector' = 'kafka',
 'topic' = 'hive_dim_temporal_output_table',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'hive_dim_temporal_output_table_group',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.enable.debug.sink' = 'true'
);


-- 最新分区要求 flink >= 1.12
SET table.sql-dialect=hive;
CREATE TABLE IF NOT EXISTS `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'streaming-source.enable'='true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '60min',
  'streaming-source.partition-order' = 'partition-name',
  'properties.enable.debug.src' = 'true'
);

insert into output_table 
select s1.channel, s1.goods_id, cast(arbitrary_udaf(d.name) as string) as name, d.dt 
from MyKafkaSrc01 s1 
left join `myhive`.`default`.MyHiveDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel 
group by TUMBLE(s1.proctime, INTERVAL '10' SECONDS), s1.channel, s1.goods_id, d.dt;


/**
insert into print_table_1 
select s1.channel, s1.goods_id, d.name, d.dt from MyKafkaSrc01 s1 
join `myhive`.`default`.MyHiveDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel;

*/
