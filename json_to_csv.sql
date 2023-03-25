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
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table kafka_stock_order_confirm (
    buy map<string,string>,
    sell map<string,string>
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


create table kafka_stock_order_result (
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
 'topic' = 'stock_order_result',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_result_group',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset'
);


create table kafka_stock_order_confirm_result (
    confirm_order_no bigint,
    sec_code string,
    ts_timestamp timestamp
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm_result',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'stock_order_confirm_result_group',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset'
);

insert into kafka_stock_order_result select * from kafka_stock_order;

insert into kafka_stock_order_confirm_result select cast(buy['order_no'] as bigint), buy['sec_code'], bigint_to_ts(buy['ts']) from kafka_stock_order_confirm;

    