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
 'topic' = 'stock_order_log_compacted',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_log_compacted_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


CREATE TABLE kafka_alert_price_change(
  sec_code STRING,
  acc_id_arr string,
  output_window_start string,
  output_window_end string,
  be_acc boolean
) WITH (
  'connector' = 'kafka',
  'topic' = 'alert_price_change_log_compacted',
  'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
  'properties.group.id' = 'alert_price_change_log_compacted_group',
 'format' = 'json',
  'scan.startup.mode' = 'latest-offset',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'SQL'
);


CREATE FUNCTION price_change_bucket_udaf AS 'com.xxx.streamx.app.func.PriceChangeBucketUdaf' LANGUAGE JAVA;

CREATE FUNCTION price_change_merge_udtaf AS 'com.xxx.streamx.app.func.PriceChangeMergeUdtaf' LANGUAGE JAVA;
/***/

CREATE VIEW view_sec_code_mod AS 
select sec_code, price_change_bucket_udaf(order_type, acct_id, trade_dir, order_price, order_vol, ts_timestamp) as map,
cast(TUMBLE_START(ts_timestamp, INTERVAL '10' SECONDS) as string) as window_start,
cast(TUMBLE_END(ts_timestamp, INTERVAL '10' SECONDS) as string) as window_end
from kafka_stock_order 
group by TUMBLE(ts_timestamp, INTERVAL '10' SECONDS), sec_code, MOD(HASH_CODE(acct_id), 2048);



insert into kafka_alert_price_change 
udtaf select price_change_merge_udtaf(sec_code, map, window_start, window_end) as (sec_code, acc_id_arr, output_window_start, output_window_end) 
from view_sec_code_mod group by window_start, window_end;

