SET table.sql-dialect=default;
create table avro_data_topic(
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    order_no bigint COMMENT '原始订单参考编号',
    sec_code string comment '产品代码',
    trade_dir string COMMENT '交易方向,B 或者 S',
    order_price double comment '交易价格，单位为分',
    order_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',
    act_no bigint COMMENT '订单确认顺序号',
    withdraw_order_no bigint COMMENT '撤单订单编号',
    pbu double COMMENT '发出此订单的报盘机编号',
    order_status string COMMENT '订单状态,0=New,1=Cancelled,2=Reject',
    ts bigint COMMENT '订单接收时间，微妙级时间戳',
    ts_timestamp AS bigint_to_ts(ts),
    proctime AS PROCTIME()
) 
with (
 'connector' = 'kafka',
 'topic' = 'avro_data_topic',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_group',
 'format' = 'avro',
 'scan.startup.mode' = 'latest-offset',
 'properties.enable.debug.src' = 'true'
);

CREATE TABLE mysql_avg_price (
    acct_id STRING,
    avg_price double,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (acct_id, output_window_start, output_window_end) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_avg_price',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '1min'
);


CREATE CATALOG myhive WITH ('be.hive.catalog'='true', 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default');

SET table.sql-dialect=hive;
CREATE TABLE IF NOT EXISTS  `myhive`.`default`.MyHiveDimTable (
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
  'streaming-source.monitor-interval'='1 min',
  'streaming-source.partition-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-10',
  'lookup.join.cache.ttl'='1 min'
);

SET table.sql-dialect=default;
CREATE TABLE kafka_scene_05_output (
    acct_id STRING,
    avg_price double,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (acct_id, output_window_start, output_window_end) NOT ENFORCED
) WITH (
 'connector' = 'kafka',
 'topic' = 'scene_05_output',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'scene_05_output_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.enable.debug.sink' = 'true'
);


CREATE TABLE print_table(
    acct_id STRING,
    avg_price double,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (acct_id, output_window_start, output_window_end) NOT ENFORCED
) WITH ('connector' = 'print');


insert into print_table 
SELECT acct_id, AVG(order_price) as avg_price,
cast(HOP_START(proctime, INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS) as string) as output_window_start,
cast(HOP_END(proctime, INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS) as string) as output_window_end
FROM avro_data_topic  
GROUP BY HOP(proctime, INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS), acct_id;


