SET table.sql-dialect=default;
CREATE TABLE MyKafkaSrc01(
    channel STRING,
    goods_id INT,
    dt STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka-0.11',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
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
 'connector' = 'kafka-0.11',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'MyKafkaSrc02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);

CREATE TABLE MyKafkaSink01(
channel STRING,
goods_id INT,
pv INT,
order_counts INT,
name STRING,
dt STRING
)
WITH (
'connector' = 'kafka-0.11',
'topic' = 'MyKafkaSink01',
'properties.bootstrap.servers' = '192.168.2.201:9092',
'properties.group.id' = 'MyKafkaSink01',
'format' = 'csv',
'scan.startup.mode' = 'latest-offset',
'csv.ignore-parse-errors' = 'true'
);

SET table.sql-dialect=hive;
CREATE TABLE MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'streaming-source.enable'='true',
  'streaming-source.monitor-interval'='1 m',
  'streaming-source.consume-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-17',
  'lookup.join.cache.ttl'='1 min',
  'lookup.cache.max-rows' = '5000',
  'lookup.cache.ttl' = '1min'
);

set next.query.sql.convert.retract.stream=true;

insert into MyKafkaSink01
select s1.channel, s1.goods_id, 1 as pv, 1 as order_counts, d.name, s1.dt 
from MyKafkaSrc01 AS s1,
LATERAL TABLE (hive_dim_dt(s1.proctime)) AS d
where s1.channel = d.channel and s1.dt = d.dt
group by s1.channel, s1.goods_id, s1.dt, d.name;


