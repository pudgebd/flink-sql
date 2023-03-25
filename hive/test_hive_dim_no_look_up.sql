CREATE CATALOG myhive WITH ('be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default');


SET table.sql-dialect=default;
CREATE TABLE MyKafkaSrc01(
    channel STRING,
    goods_id INT,
    dt STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
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
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'MyKafkaSrc02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);


CREATE TABLE MyKafkaSink01(
channel STRING,
counts BIGINT,
name STRING,
dt STRING
) WITH (
'connector' = 'kafka',
'topic' = 'MyKafkaSink01',
'properties.bootstrap.servers' = '192.168.2.201:9092',
'properties.group.id' = 'MyKafkaSink01',
'format' = 'csv',
'scan.startup.mode' = 'latest-offset',
'csv.ignore-parse-errors' = 'true'
);


CREATE TABLE print_table(
channel STRING,
counts BIGINT,
name STRING,
dt STRING
) WITH ('connector' = 'print');

/**
SET table.sql-dialect=hive;
CREATE TABLE `myhive`.`bigdata_test`.TestOtherDbMyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/bigdata_test.db/testotherdbmyhivedimtable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 d',
  'sink.partition-commit.policy.kind'='metastore,success-file',
  'streaming-source.enable'='true',
  'streaming-source.partition-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-10'
);
*/

CREATE TABLE  `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 d',
  'sink.partition-commit.policy.kind'='metastore,success-file',
  'streaming-source.enable'='true',
  'streaming-source.partition-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-10'
);

/**
ALTER TABLE `myhive`.`default`.MyHiveDimTable SET TBLPROPERTIES('streaming-source.monitor-interval' = '30 seconds');
*/

-- `myhive`.`default`.MyHiveDimTable
-- `myhive`.`bigdata_test`.TestOtherDbMyHiveDimTable
insert into print_table 
select s1.channel, count(s1.goods_id) as counts, cast(arbitrary_udaf(d.name) as string) as name, s1.dt from MyKafkaSrc01 s1 
left join `myhive`.`default`.MyHiveDimTable /*+ OPTIONS('streaming-source.monitor-interval'='30 seconds') */ d on s1.channel = d.channel and s1.dt = d.dt 
group by s1.channel, s1.dt;

/**
set next.query.sql.convert.retract.stream=true;
set next.query.retain.retract.flag=false;

insert into MyKafkaSink01 
select s1.channel, s1.goods_id, sum(s2.pv), sum(s2.order_counts), arbitrary_udaf(d.name) as name, s1.dt from MyKafkaSrc01 s1 
inner join MyKafkaSrc02 s2 on s1.goods_id = s2.goods_id 
left join `myhive`.`default`.MyHiveDimTable d on s1.channel = d.channel and s1.dt = d.dt 
group by s1.channel, s1.goods_id, s1.dt;
*/
