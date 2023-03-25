CREATE CATALOG hive_catalog WITH (
  'type'='iceberg',
  'catalog-type'='hive',
  'warehouse'='hdfs://hdfs1:9000/user/hive/warehouse/',
  'uri'='thrift://hdfs2:9083',  
  'property-version'='1'
);


CREATE TABLE print_table(
    a string,
    b string,
    c string,
    d string,
    ts_millis_to_timestamp_01 timestamp, 
    sql_date_to_timestamp_01 timestamp, 
    iso8601_date_to_timestamp_01 timestamp, 
    ts_ltz_date_to_timestamp_01 timestamp,
    ts_millis_to_timestamp_02 timestamp, 
    sql_date_to_timestamp_02 timestamp, 
    iso8601_date_to_timestamp_02 timestamp, 
    ts_ltz_date_to_timestamp_02 timestamp
) WITH ('connector' = 'print');


insert into print_table select a, b, c, d, ts_millis_to_timestamp_01, sql_date_to_timestamp_01, iso8601_date_to_timestamp_01, ts_ltz_date_to_timestamp_01, ts_millis_to_timestamp_02, sql_date_to_timestamp_02, iso8601_date_to_timestamp_02, ts_ltz_date_to_timestamp_02 from hive_catalog.xxx_localdebug_iceberg_db.t1;

