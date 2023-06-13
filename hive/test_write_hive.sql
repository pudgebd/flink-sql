CREATE CATALOG myhive WITH (
    'be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default');
/*
CREATE CATALOG myhive WITH (
    'be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default', 
    'hive.metastore.kerberos.principal.real' = 'work@xxx.COM',
    'hive.metastore.kerberos.keytab.file.path' = '/home/work/work.keytab', 
    'create.hive.catalog.timeout.seconds' = '5','java.security.krb5.conf' = '/etc/krb5.conf');
    */

SET table.sql-dialect=default;
create table MyKafkaSrc01(
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    order_no bigint COMMENT '原始订单参考编号',
    sec_code string comment '产品代码',
    trade_dir string COMMENT '交易方向,B 或者 S',
    order_price bigint comment '交易价格，单位为分',
    order_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',
    act_no bigint COMMENT '订单确认顺序号',
    withdraw_order_no bigint COMMENT '撤单订单编号',
    pbu double COMMENT '发出此订单的报盘机编号',
    order_status string COMMENT '订单状态,0=New,1=Cancelled,2=Reject',
    ts bigint COMMENT '订单接收时间，微妙级时间戳',
    ts_timestamp AS bigint_to_ts(ts),
    WATERMARK FOR ts_timestamp AS ts_timestamp - INTERVAL '5' SECONDS
    -- proctime as PROCTIME()
) 
with (
 'connector' = 'kafka',
 'topic' = 'stock_order',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


CREATE TABLE MyDimTable (
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql_9w',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '1 hour'
 );


CREATE TABLE print_table_01 (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    order_no bigint COMMENT '原始订单参考编号',
    sec_code string comment '产品代码',
    trade_dir string COMMENT '交易方向,B 或者 S',
    order_price bigint comment '交易价格，单位为分',
    order_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',
    act_no bigint COMMENT '订单确认顺序号',
    withdraw_order_no bigint COMMENT '撤单订单编号',
    pbu double COMMENT '发出此订单的报盘机编号',
    order_status string COMMENT '订单状态,0=New,1=Cancelled,2=Reject',
    ts bigint COMMENT '订单接收时间，微妙级时间戳',
    score double,
    dt string, 
    hour_parti string, 
    minute_parti string
) WITH ('connector' = 'print');


SET table.sql-dialect=hive;
CREATE TABLE `myhive`.`default`.FlinkWriteHiveTable (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    order_no bigint COMMENT '原始订单参考编号',
    sec_code string comment '产品代码',
    trade_dir string COMMENT '交易方向,B 或者 S',
    order_price bigint comment '交易价格，单位为分',
    order_vol bigint comment '含3位小数，比如数量为100股，则交易数量为二进制100000',
    act_no bigint COMMENT '订单确认顺序号',
    withdraw_order_no bigint COMMENT '撤单订单编号',
    pbu double COMMENT '发出此订单的报盘机编号',
    order_status string COMMENT '订单状态,0=New,1=Cancelled,2=Reject',
    ts bigint COMMENT '订单接收时间，微妙级时间戳',
    score double
) PARTITIONED BY (dt String, hour_parti string, minute_parti string) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/flinkwritehivetable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt $hour_parti:$minute_parti:00',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 minute', -- flink-conf.yaml 要配置 execution.checkpointing.interval: 60000
  'sink.partition-commit.policy.kind'='metastore,success-file',
  'sink.rolling-policy.file-size' = '1MB', -- 每个文件最大MB
  'sink.rolling-policy.rollover-interval' = '30 min',
  'sink.rolling-policy.check-interval' = '1 min'
);


insert into `myhive`.`default`.FlinkWriteHiveTable 
select s1.order_type, s1.acct_id, s1.order_no, s1.sec_code, s1.trade_dir, s1.order_price, 
s1.order_vol, s1.act_no, s1.withdraw_order_no, s1.pbu, s1.order_status, s1.ts, 
s1.order_vol as score, DATE_FORMAT(ts_timestamp, 'yyyy-MM-dd') as dt, 
DATE_FORMAT(ts_timestamp, 'HH') as hour_parti, DATE_FORMAT(ts_timestamp, 'mm') as minute_parti
from MyKafkaSrc01 s1;
