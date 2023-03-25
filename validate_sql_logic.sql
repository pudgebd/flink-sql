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
    ts_sql timestamp(3) COMMENT '订单接收时间，微妙级时间戳',
    ts_iso timestamp(3) COMMENT '订单接收时间，微妙级时间戳',
    ts_long bigint COMMENT '订单接收时间，微妙级时间戳',
    ts as bigint_to_ts(ts_long)
) 
with (
 'connector' = 'kafka',
 'topic' = 'stock_order',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
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
    ts as bigint_to_ts(buy['ts_long']),
    max_price bigint
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'stock_order_confirm_origin_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_stock_detail (
    sec_code string,
    descr string,
    create_user string,
    ts timestamp(3)
) with (
 'connector' = 'kafka',
 'topic' = 'stock_detail',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'stock_detail_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_stock_history (
    sec_code string,
    max_price bigint,
    min_price bigint,
    create_user string
) with (
 'connector' = 'kafka',
 'topic' = 'stock_history',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'stock_history_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_sec_code_event_counts (
    sec_code string,
    event_counts int,
    ts bigint
) 
with (
 'connector' = 'kafka',
 'topic' = 'sec_code_event_counts',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'sec_code_event_counts_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


/**
insert into kafka_sec_code_event_counts 
select cd.sec_code, count(create_user) from kafka_stock_order o left join (
  (
    select c.sec_code, d.create_user from kafka_stock_order_confirm c 
    left join kafka_stock_detail d on c.sec_code = d.sec_code 
    where d.ts > '2020-02-02 11:11:11' 
  ) 
  union
  (
    select c.sec_code, h.create_user from kafka_stock_order_confirm c 
    inner join kafka_stock_history h on c.sec_code = h.sec_code 
    where h.max_price > 9999
  )
) cd 
on o.sec_code = cd.sec_code group by acct_id;


insert into kafka_sec_code_event_counts 
    select c.sec_code, d.create_user from kafka_stock_order_confirm c 
    left join kafka_stock_detail d on c.sec_code = d.sec_code 
    where ts > '2020-02-02 11:11:11' 
union
    select c.sec_code, h.create_user from kafka_stock_order_confirm c 
    inner join kafka_stock_history h on c.sec_code = h.sec_code 
    where h.max_price > 9999;



insert into kafka_sec_code_event_counts 
select acct_id, count(order_no) from kafka_stock_order where sec_code in (
  select sec_code from kafka_stock_order_confirm where ts > '2020-02-02 11:11:11' 
) group by acct_id;



insert into kafka_sec_code_event_counts 
select acct_id, count(order_no) from kafka_stock_order o left join kafka_stock_order_confirm c on o.sec_code = c.sec_code group by o.acct_id;
*/