/**
create table complex_format_01 (
     data ARRAY<MAP<string, string>>
) with (
 'connector' = 'kafka',
 'topic' = 'complex-format',
 'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
    'properties.group.id' = 'complex_format_01',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table print_table_01_01 (
    `TenantID` string
) with (
 'connector' = 'print'
);


create table print_table_02 (
    map MAP<string, string>
) with (
 'connector' = 'print'
);

insert into print_table_02 select data[1] from complex_format_01;

insert into print_table_01_01 select data[1]['TenantID'] from complex_format_01;
*/


create table IF NOT EXISTS complex_format_02 (
     data ARRAY<ROW<`TenantID` string, `Value` string, `Tag` string, `TimeStamp` string>>
) with (
 'connector' = 'kafka',
 'topic' = 'complex-format',
 'properties.bootstrap.servers' = '10.201.0.82:9092,10.201.0.83:9092,10.201.0.84:9092',
 'properties.group.id' = 'complex_format_02',
 'format' = 'json',
 'scan.startup.mode' = 'latest-offset',
 'json.ignore-parse-errors' = 'true',
 'json.timestamp-format.standard' = 'SQL'
);


create table IF NOT EXISTS print_table_01 (
    `TenantID` string,
    `Value` string,
    `Tag` string,
    `TimeStamp` string
) with (
 'connector' = 'print'
);


insert into print_table_01 select `TenantID`, `Value`, `Tag`, `TimeStamp` from complex_format_02 CROSS JOIN UNNEST(data) AS t (`TenantID`, `Value`, `Tag`, `TimeStamp`);

-- 将 data 数组里的每个 json object 都输出到 print_table_01 表里
