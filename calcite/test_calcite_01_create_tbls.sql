create table kafka_stock_order_confirm (
    buy map<string,string>,
    sell map<string,string>,
    buy_acct_id as buy['acct_id'],
    sec_code as buy['sec_code'],
    ts as bigint_to_ts(buy['ts']),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_confirm_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


CREATE TABLE mysql_alert_self_buy_sell(
    sec_code STRING,
    alert_percent DOUBLE,
    summary BIGINT,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (sec_code, output_window_start, output_window_end) NOT ENFORCED
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'cq_alert_self_buy_sell',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '1min'
);


CREATE TABLE kafka_scene_01_output(
    sec_code STRING,
    alert_percent DOUBLE,
    summary BIGINT,
    output_window_start string,
    output_window_end string,
    PRIMARY KEY (sec_code, output_window_start, output_window_end) NOT ENFORCED 
) WITH (
 'connector' = 'kafka',
 'topic' = 'scene_01_output',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'scene_01_output_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


insert into kafka_scene_01_output 
select sec_code, alert_self_buy_sell_udtaf_v2(buy, sell), 
WINDOW_START('string') as window_start, 
WINDOW_END('string') as window_end 
from kafka_stock_order_confirm 
group by TUMBLE(ts, INTERVAL '10' SECONDS), sec_code;