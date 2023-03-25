CREATE TABLE dct_content (
  content STRING,
  tableName as cast(JSON_VALUE(content, '$.header.table') as string),
  proctime as proctime()
) WITH (
  'connector' = 'kafka',
  'topic' = 'dynamic_data_topic',
  'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
  'properties.group.id' = 'testGroup01',
  'scan.startup.mode' = 'latest-offset', 
  'format' = 'raw'
);
 

CREATE TABLE print_table (
  tableName string,
  ope string
) WITH (
  'connector' = 'print'
);


/* 报类型row不匹配错误 
insert into print_table select header, operation, location, ab_row from dct_content;
*/

/*
可以执行*/
insert into print_table select tableName, SUBSTRING(JSON_VALUE(content, '$.operation'), 1, 3) as ope from dct_content;