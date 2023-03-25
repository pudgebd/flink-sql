create table kafka_test_format(
    id bigint,
    name string
) 
with (
 'connector' = 'kafka',
 'topic' = 'test_format',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_format_group',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


