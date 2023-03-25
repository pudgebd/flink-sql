SET table.sql-dialect=default;
create table kafka_stock_order_utf8 (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    acct_name string COMMENT '',
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
 'topic' = 'stock_order',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_utf8_stock_order_group',
 'properties.serializer.encoding' = 'utf8',
 'properties.deserializer.encoding' = 'utf8',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_stock_order_gbk (
    order_type bigint COMMENT '订单类型, 0:订单；1：撤单',
    acct_id string COMMENT '投资者账户代码',
    acct_name string COMMENT '',
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
 'topic' = 'stock_order_gbk',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_gbk_stock_order_group',
 'properties.serializer.encoding' = 'gbk',
 'properties.deserializer.encoding' = 'gbk',
 'properties.key.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
 'properties.value.serializer' = 'org.apache.kafka.common.serialization.StringSerializer',
 'properties.key.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
 'properties.value.deserializer' = 'org.apache.kafka.common.serialization.StringDeserializer',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_stock_order_csv_gbk (
    acct_id string COMMENT '投资者账户代码',
    acct_name string COMMENT ''
) 
with (
 'connector' = 'kafka',
 'topic' = 'stock_order_csv_gbk',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_gbk_csv_stock_order_group',
 'properties.serializer.encoding' = 'gbk',
 'properties.deserializer.encoding' = 'gbk',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset'
);


create table test_utf8_join_gbk_output (
    utf8_acct_name string COMMENT '',
    gbk_acct_name string COMMENT ''
) 
with (
 'connector' = 'kafka',
 'topic' = 'test_utf8_join_gbk_output',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_utf8_join_gbk_output_group',
 'properties.serializer.encoding' = 'utf8',
 'properties.deserializer.encoding' = 'utf8',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE print_table (
    acct_name1 STRING,
    acct_name2 STRING
) WITH ('connector' = 'print');

insert into print_table select acct_name, acct_name from kafka_stock_order_gbk;

/**
insert into test_utf8_join_gbk_output 
SELECT u.acct_name, g.acct_name, 
cast(TUMBLE_START(proctime, INTERVAL '5' SECONDS) as string) as output_window_start,
cast(TUMBLE_END(proctime, INTERVAL '5' SECONDS) as string) as output_window_end
FROM kafka_stock_order_utf8 u inner join kafka_stock_order_gbk g on u.acct_name = g.acct_name 
GROUP BY TUMBLE(proctime, INTERVAL '5' SECONDS), u.acct_name;
*/

