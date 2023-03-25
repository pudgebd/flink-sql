create table kafka_stock_order_confirm_origin (
    buy map<string,string>,
    sell map<string,string>,
    sec_code as buy['sec_code'],
    ts as bigint_to_ts(buy['ts']),
    WATERMARK FOR ts AS ts - INTERVAL '5' SECONDS
) with (
 'connector' = 'kafka',
 'topic' = 'stock_order_confirm_origin',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'stock_order_confirm_origin_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE print_table (
    sec_code string,
    ts timestamp(3)
) WITH ('connector' = 'print');

insert into print_table select sec_code, ts from kafka_stock_order_confirm_origin;