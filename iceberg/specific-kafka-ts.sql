CREATE TABLE dct_content (
  header map<string, string>,
  operation string, 
  location map<string, string>,
  proctime as proctime()
) WITH (
  'connector' = 'kafka',
  'topic' = 'dynamic_data_topic',
  'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
  'properties.group.id' = 'asfsdfgs-05',
  'scan.startup.mode' = 'timestamp', 
  'scan.startup.timestamp-millis' = '1662515339835',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'SQL'
);
 

CREATE TABLE print_table (
  tableKey string,
  operation string, 
  b string,
  sql_date string
) WITH (
  'connector' = 'print'
);


insert into print_table select header['table'] as tableKey, operation, location['b'] as b, location['sql_date'] as sql_date from dct_content where header['table'] = 't1';