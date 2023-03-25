CREATE CATALOG myhive WITH ('be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default');


SET table.sql-dialect=default;
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

create table kafka_stock_order_confirm (
    buy map<string,string>,
    sell map<string,string>,
    confirm_order_no as cast(buy['order_no'] as bigint),
    sec_code as buy['sec_code'],
    trade_price as buy['trade_price'],
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


CREATE TABLE print_table (
    sec_code string,
    event_counts double
) WITH ('connector' = 'print');


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

insert into print_table 
select o.sec_code, avg(cast(c.trade_price as bigint)) 
from kafka_stock_order o left join kafka_stock_order_confirm c 
on o.sec_code = c.sec_code 
left join `myhive`.`default`.MyHiveDimTable FOR SYSTEM_TIME AS OF o.ts_timestamp AS d on o.acct_id = d.channel 
where o.order_status = '0' 
group by HOP(o.ts_timestamp, INTERVAL '5' SECONDS, INTERVAL '10' SECONDS), o.sec_code;


/**
insert into print_table_1 
select s1.channel, s1.goods_id, d.name, d.dt from MyKafkaSrc01 s1 
join `myhive`.`default`.MyHiveDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel;

*/
