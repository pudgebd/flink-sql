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
    ts bigint COMMENT '订单接收时间，微妙级时间戳'
) 
with (
 'connector' = 'kafka',
 'topic' = 'stock_order',
 'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_stock_order_confirm (
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
 
 /*
 WITH(
 'connector' = 'upsert-kafka',
 'topic' = 'scene_03_output',
 'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
 'key.format' = 'raw',
 'value.format' = 'json',
 'value.json.ignore-parse-errors' = 'true',
 'value.json.timestamp-format.standard' = 'SQL'
 );
 */

CREATE TABLE CqDimMysql(
    channel STRING,
    name STRING,
    score double
 ) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://10.201.0.205:3306/test?charset=utf8',
   'table-name' = 'cq_dim_mysql',
   'username' = 'root',
   'password' = '123456',
   'lookup.cache.max-rows' = '1000',
   'lookup.cache.ttl' = '5 sec'
 );

/*
create view my_view (sec_code, trade_price, trade_vol, buy_acct_id, sell_acct_id, score) as 
select oc.sec_code, first_value(oc.trade_price) as trade_price, first_value(oc.trade_vol) as trade_vol, first_value(oc.buy_acct_id) as buy_acct_id, first_value(oc.sell_acct_id) as sell_acct_id, first_value(hd.score) as score from kafka_stock_order_confirm oc 
left join CqDimMysql hd on oc.sec_code = hd.channel group by oc.sec_code;
*/

create view my_view (sec_code, trade_price, trade_vol, buy_acct_id, sell_acct_id, score) as 
select oc.sec_code, first_value(oc.trade_price) as trade_price, first_value(oc.trade_vol) as trade_vol, first_value(oc.buy_acct_id) as buy_acct_id, first_value(oc.sell_acct_id) as sell_acct_id, 0.1 as score from kafka_stock_order_confirm oc
group by TUMBLE(oc.proctime, INTERVAL '30' SECONDS), oc.sec_code;

--  FOR SYSTEM_TIME AS OF oc.proctime AS

insert into kafka_scene_03_output select sec_code, score, substring(buy_acct_id, 0, 1) as buy_acct_id, (cast(trade_vol as bigint) - 100) as trade_vol from my_view where score > 0.03;
