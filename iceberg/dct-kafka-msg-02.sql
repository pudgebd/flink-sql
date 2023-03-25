CREATE TABLE dct_content (
    content STRING,
    __kafka_offset__ bigint METADATA FROM 'offset' VIRTUAL,
    __kafka_topic__ string METADATA FROM 'topic' VIRTUAL,
    __processing_ts__ timestamp METADATA FROM 'timestamp' VIRTUAL,
    __processing_time__ as ts_to_bigint(__processing_ts__),
    __processing_date__ as DATE_FORMAT(__processing_ts__, 'yyyy-MM-dd'),
    a as cast(JSON_VALUE(content, '$.location.a') as string),
    b as cast(JSON_VALUE(content, '$.location.b') as string),
    c as cast(JSON_VALUE(content, '$.location.c') as bigint),
    operation as JSON_VALUE(content, '$.operation'),
    header_map as MAP[JSON_VALUE(content, '$.header.catalog'), JSON_VALUE(content, '$.header.table')],
    ab_array as ARRAY[JSON_VALUE(content, '$.location.a'), JSON_VALUE(content, '$.location.b')],
    ab_decimal as cast((__kafka_offset__ / 2) as decimal),
    a_binary as ENCODE(JSON_VALUE(content, '$.location.a'), 'UTF-8'),
    b_fixed as ENCODE(JSON_VALUE(content, '$.location.b'), 'UTF-8'),
    uuid as uuid(),
    ts as proctime(),
    c_time as DATE_FORMAT(__processing_ts__, 'HH:mm:ss.SSS'),
    cur_date as cast(DATE_FORMAT(__processing_ts__, 'dd') as int),
    a_double as cast((__kafka_offset__ / 3) as double),
    a_float as cast((__kafka_offset__ / 3) as float),
    __kafka_partition__ int METADATA FROM 'partition' VIRTUAL,
    offset_boolean as CASE WHEN __kafka_offset__ / 2 = 0 then true else false end
) WITH (
    'connector' = 'kafka',
    'topic' = 'dynamic_data_topic',
    'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
    'properties.group.id' = 'testGroup02',
    'scan.startup.mode' = 'latest-offset', 
    'format' = 'raw'
);



CREATE TABLE print_table_01 (
    b string,
    __kafka_offset__ bigint,
    __kafka_partition__ int,
    __kafka_topic__ string,
    __processing_time__ bigint,
    __processing_date__ string,
    header_map map<string, string>,
    ab_array array<string>,
    ab_decimal decimal(10, 3),    
    a_binary binary,
    b_fixed binary,
    uuid string,
    ts timestamp,
    c_time string,
    cur_date int,
    a_double double,
    a_float float,
    partition_int int,
    offset_boolean boolean,
    ab_row row<a string, b string>
) WITH (
    'connector' = 'print'
);

CREATE TABLE print_table_02 (
    b string,
    c bigint
) WITH (
    'connector' = 'print'
);
	
/**/
insert into print_table_01 select b, __kafka_offset__, __kafka_partition__, __kafka_topic__, __processing_time__, __processing_date__, header_map, ab_array, ab_decimal, a_binary, b_fixed, uuid, ts, c_time, cur_date, a_double, a_float, __kafka_partition__, offset_boolean, 
ROW(JSON_VALUE(content, '$.location.a'), JSON_VALUE(content, '$.location.b')) as ab_row from dct_content;

/*
insert into print_table_02 select b, c from dct_content;
*/