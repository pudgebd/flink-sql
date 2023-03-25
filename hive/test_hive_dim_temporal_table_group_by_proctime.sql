CREATE CATALOG myhive WITH ('be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default');


SET table.sql-dialect=default;
CREATE TABLE MyKafkaSrc01(
    channel STRING,
    goods_id INT,
    dt string,
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


CREATE TABLE MyKafkaSrc02(
    goods_id INT,
    pv INT,
    order_counts INT
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'MyKafkaSrc02',
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


CREATE TABLE print_table_2 (
  channel STRING,
  goods_id INT,
  sum_pv INT,
  sum_order_counts INT,
  name STRING,
  dt STRING
) WITH ('connector' = 'print');


/**
SET table.sql-dialect=hive;
CREATE TABLE  `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'streaming-source.enable'='true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '1 min',
  'streaming-source.partition-order' = 'partition-time',
  'partition.time-extractor.kind' = 'default',
  'partition.time-extractor.timestamp-pattern' = '$dt 00:00:00'
);


SET table.sql-dialect=hive;
CREATE TABLE  `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'streaming-source.enable'='true',
  'streaming-source.partition.include' = 'latest',
  'streaming-source.monitor-interval' = '1 min',
  'streaming-source.partition-order' = 'partition-name'
);


SET table.sql-dialect=hive;
CREATE TABLE  `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'streaming-source.enable'='false',
  'streaming-source.partition.include' = 'all',
  'lookup.join.cache.ttl' = '1 min'
);

*/

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
  'streaming-source.partition-order' = 'partition-name'
);

insert into print_table_2 
select s1.channel, s1.goods_id, sum(s2.pv), sum(s2.order_counts), cast(arbitrary_udaf(d.name) as string) as name, d.dt from MyKafkaSrc01 s1 
inner join MyKafkaSrc02 s2 on s1.goods_id = s2.goods_id 
left join `myhive`.`default`.MyHiveDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel 
group by TUMBLE(s1.proctime, INTERVAL '10' SECONDS), s1.channel, s1.goods_id, d.dt;


/**
insert into print_table_1 
select s1.channel, s1.goods_id, d.name, d.dt from MyKafkaSrc01 s1 
join `myhive`.`default`.MyHiveDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel;

*/
