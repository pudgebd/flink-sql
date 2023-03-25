CREATE TABLE dct_content (
  header map<string, string>,
  operation string, 
  location map<string, string>,
  ab_row as row('a', 'b'),
  proctime as proctime()
) WITH (
  'connector' = 'kafka',
  'topic' = 'dynamic_data_topic',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup01',
  'scan.startup.mode' = 'latest-offset', 
  'format' = 'json',
  'json.ignore-parse-errors' = 'true',
  'json.timestamp-format.standard' = 'SQL'
);
 

CREATE TABLE print_table (
  header map<string, string>,
  operation string, 
  location map<string, string>,
  ab_row row<a string, b string>
) WITH (
  'connector' = 'print'
);


/* 报类型row不匹配错误 
insert into print_table select header, operation, location, ab_row from dct_content;
*/

/*
可以执行
insert into print_table select header, operation, location, ROW(JSON_VALUE(content, '$.location.a'), JSON_VALUE(content, '$.location.b')) as ab_row from dct_content;
*/
insert into print_table select header, operation, location, ROW('a', 'b') as ab_row from dct_content;