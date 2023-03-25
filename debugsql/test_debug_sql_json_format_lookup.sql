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
    proctime as PROCTIME()
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


create table kafka_stock_order_confirm (
    buy map<string,string>,
    sell map<string,string>,
    confirm_order_no as cast(buy['order_no'] as bigint),
    sec_code as buy['sec_code'],
    ts_timestamp as bigint_to_ts(buy['ts'])
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_confirm_origin_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
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
 'json.timestamp-format.standard' = 'SQL'
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
 'json.timestamp-format.standard' = 'SQL'
);


CREATE TABLE `default_catalog`.`default_database`.MyDimTable (
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'cq_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '10 sec'
 );


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
 'json.timestamp-format.standard' = 'SQL'
);

-- earliest
/**
CREATE FUNCTION sec_code_event_udaf AS 'com.xxx.streamx.app.func.SecCodeEventUdaf' LANGUAGE JAVA;
left join MyDimTable d on o.sec_code = d.channel 
*/

set next.query.sql.convert.retract.stream=true;
set next.query.retain.retract.flag=true;

insert into kafka_stock_after_join_write 
select o.order_type, o.sec_code, o.ts, o.order_no, c.confirm_order_no from kafka_stock_order o 
left join kafka_stock_order_confirm c on o.sec_code = c.sec_code and o.ts_timestamp = c.ts_timestamp 
left join MyDimTable FOR SYSTEM_TIME AS OF o.proctime AS d on o.sec_code = d.channel 
where o.order_status = '0';

/***/
insert into kafka_scene_06_output
select sec_code, 
sec_code_event_udaf(order_type, sec_code, order_no, confirm_order_no, be_acc) as event_counts from kafka_stock_after_join_read 
group by HOP(ts_timestamp, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS), sec_code;

