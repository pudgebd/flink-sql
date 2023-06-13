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
    proctime as PROCTIME(),
    WATERMARK FOR ts_timestamp AS ts_timestamp - INTERVAL '5' SECONDS
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


CREATE TABLE MyKafkaSrc02(
    channel STRING PRIMARY KEY NOT ENFORCED,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup02',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset'
);

/**
CREATE TABLE MyKafkaSrc02(
    channel STRING PRIMARY KEY NOT ENFORCED,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);
*/

-- 有个id主键，此处可不定义
CREATE TABLE `default_catalog`.`default_database`.MyDimTable(
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '10 sec'
 );


CREATE TABLE KafkaToWrite(
    channel STRING,
    score double
) WITH (
 'connector' = 'kafka',
 'topic' = 'test_write_to_kafka',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_write_to_kafka_group',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);

CREATE TABLE UpsertKafkaToWrite(
    channel STRING PRIMARY KEY NOT ENFORCED,
    score double
) WITH (
 'connector' = 'upsert-kafka',
 'topic' = 'test_write_to_kafka',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_write_to_kafka_group',
 'key.format' = 'raw',
 'value.format' = 'csv',
 'value.csv.ignore-parse-errors' = 'true'
);

-- Temporal
insert into UpsertKafkaToWrite select s1.sec_code, sum(s1.order_vol) as score from MyKafkaSrc01 s1 left join MyDimTable FOR SYSTEM_TIME AS OF s1.proctime AS s2 on s1.sec_code = s2.channel 
group by TUMBLE(s1.proctime, INTERVAL '1' SECONDS), s1.sec_code;


/**



-- Interval
insert into UpsertKafkaToWrite select s1.sec_code, sum(s1.order_vol) as score from MyKafkaSrc01 s1 left join MyKafkaSrc02 s2 on s1.sec_code = s2.channel 
and s1.ts_timestamp BETWEEN s2.proctime - INTERVAL '1' MINUTE AND s2.proctime 
group by TUMBLE(s1.ts_timestamp, INTERVAL '1' SECONDS), s1.sec_code;

-- Regular
insert into UpsertKafkaToWrite select s1.sec_code, sum(s1.order_vol) as score from MyKafkaSrc01 s1 left join MyKafkaSrc02 s2 on s1.sec_code = s2.channel 
group by s1.sec_code;

*/


