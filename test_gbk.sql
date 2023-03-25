SET table.sql-dialect=default;
create table kafka_stock_order(
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
 'properties.group.id' = 'stock_order_group_dsf-wer3-dfsg-265c3',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL',
 'properties.enable.debug.src' = 'true'
);

CREATE TABLE mysql_avg_price (
    acct_id STRING,
    acct_name STRING,
    avg_price double,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (acct_id, output_window_start, output_window_end) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/utf8?useUnicode=true&characterEncoding=UTF-8',
   'table-name' = 'cq_avg_price',
   'username' = 'root',
   'password' = '123456',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '1min'
);


CREATE TABLE mysql_avg_price_gbk (
    acct_id STRING,
    acct_name STRING,
    avg_price double,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (acct_id, output_window_start, output_window_end) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://localhost:3306/gbk?useUnicode=true&characterEncoding=gbk',
   'table-name' = 'cq_avg_price_gbk',
   'username' = 'root',
   'password' = '123456',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '1min'
);


CREATE TABLE print_table(
    acct_id STRING,
    acct_name STRING,
    avg_price double,
    output_window_start string,
    output_window_end string
) WITH ('connector' = 'print');


CREATE FUNCTION gbk_to_utf8 AS 'com.xxx.streamx.app.common.func.GbkToUtf8' LANGUAGE JAVA;


insert into mysql_avg_price_gbk 
SELECT acct_id, gbk_to_utf8(cast(arbitrary_udaf(acct_name) as string)), AVG(order_price) as avg_price,
cast(TUMBLE_START(proctime, INTERVAL '5' SECONDS) as string) as output_window_start,
cast(TUMBLE_END(proctime, INTERVAL '5' SECONDS) as string) as output_window_end
FROM kafka_stock_order 
GROUP BY TUMBLE(proctime, INTERVAL '5' SECONDS), acct_id;


