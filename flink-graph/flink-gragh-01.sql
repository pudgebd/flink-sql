create table kafka_order_confirm (
    buy map<string,string>,
    sell map<string,string>,
    confirm_order_no as buy['order_no'],
    sec_code as buy['sec_code'],
    trade_vol as buy['trade_vol'],
    trade_price as buy['trade_price'],
    buy_acct_id as buy['acct_id'],
    sell_acct_id as sell['acct_id'],
    proctime as proctime()
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm',
 'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
    'properties.group.id' = 'complex_format_01',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


CREATE TABLE kafka_scene_03_output(
    sec_code STRING PRIMARY KEY,
    score DOUBLE,
    buy_acct_id STRING,
    trade_vol bigint
 )
 WITH ('connector' = 'print');
 

create view my_view (sec_code, trade_price, trade_vol, buy_acct_id, sell_acct_id, score) as 
select oc.sec_code, first_value(oc.trade_price) as trade_price, first_value(oc.trade_vol) as trade_vol, first_value(oc.buy_acct_id) as buy_acct_id, first_value(oc.sell_acct_id) as sell_acct_id, 0.1 as score from kafka_order_confirm oc
group by TUMBLE(oc.proctime, INTERVAL '30' SECONDS), oc.sec_code;

--  FOR SYSTEM_TIME AS OF oc.proctime AS

insert into kafka_scene_03_output select sec_code, score, substring(buy_acct_id, 0, 1) as buy_acct_id, (cast(trade_vol as bigint) - 100) as trade_vol from my_view where score > 0.03;
