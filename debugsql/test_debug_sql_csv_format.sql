create table kafka_stock_order(
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
    ts_timestamp as bigint_to_ts(ts),
    proctime AS proctime()
) 
with (
 'connector' = 'kafka',
 'topic' = 'stock_order',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


create table kafka_stock_order_confirm (
    confirm_order_no bigint,
    sec_code string,
    ts_timestamp timestamp
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_confirm_origin_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


create table kafka_stock_after_join_write (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    sec_code string comment '产品代码',
    ts bigint,
    order_no bigint,
    confirm_order_no bigint,
    be_acc boolean
) 
with (
 'connector' = 'kafka',
 'topic' = 'num_07_stock_after_join',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'num_07_stock_after_join_write_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


create table kafka_stock_after_join_read (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    sec_code string comment '产品代码',
    ts bigint,
    order_no bigint,
    confirm_order_no bigint,
    be_acc boolean,
    ts_timestamp as bigint_to_ts(ts),
    WATERMARK FOR ts_timestamp AS ts_timestamp - INTERVAL '5' SECONDS
) 
with (
 'connector' = 'kafka',
 'topic' = 'num_07_stock_after_join',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'num_07_after_join_read_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);




CREATE TABLE `default_catalog`.`default_database`.MyDimTable (
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = '50000',
   'lookup.cache.ttl' = '10 sec'
 );

/**
CREATE CATALOG myhive WITH ('be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default', 'hive.metastore.kerberos.principal' = 'hive/cdh601@xxx.COM', 'hive.metastore.kerberos.keytab.file' = '/home/work/hive_cdh601.keytab');

SET table.sql-dialect=hive;
CREATE TABLE `myhive`.`default`.myDimTable (
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
  'streaming-source.consume-start-offset'='2020-11-10',
  'lookup.join.cache.ttl'='1 min'
);
*/

SET table.sql-dialect=default;
create table kafka_scene_06_output (
    sec_code string,
    event_counts int
) 
with (
 'connector' = 'kafka',
 'topic' = 'scene_06_output',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'scene_06_output_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);

-- earliest
/**
CREATE FUNCTION sec_code_event_udaf AS 'com.xxx.streamx.app.func.SecCodeEventUdaf' LANGUAGE JAVA;

*/

/***/
set next.query.sql.convert.retract.stream=true;
set next.query.retain.retract.flag=true;


insert into kafka_stock_after_join_write 
select o.order_type, o.sec_code, o.ts, o.order_no, c.confirm_order_no from kafka_stock_order o 
left join kafka_stock_order_confirm c on o.sec_code = c.sec_code and o.ts_timestamp = c.ts_timestamp 
inner join MyDimTable FOR SYSTEM_TIME AS OF o.proctime AS d on o.sec_code = d.channel 
where o.order_status = '0';

-- MyDimTable FOR SYSTEM_TIME AS OF o.proctime AS 

/***/
insert into kafka_scene_06_output
select sec_code, 
sec_code_event_udaf(order_type, sec_code, order_no, confirm_order_no, be_acc) as event_counts from kafka_stock_after_join_read 
group by HOP(ts_timestamp, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS), sec_code;

