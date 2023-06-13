CREATE TABLE dct_msg (
  msg STRING,
  tableName as cast(JSON_VALUE(msg, '$.header.table') as string),
  ab_row as row['a', 'b'],
  proctime as proctime()
) WITH (
  'connector' = 'kafka',
  'topic' = 'dynamic_data_topic',
  'properties.bootstrap.servers' = 'hdfs1:9092,hdfs2:9092,hdfs3:9092',
  'properties.group.id' = 'testGroup01',
  'scan.startup.mode' = 'latest-offset', 
  'format' = 'raw'
);

CREATE TABLE MyDimMysql(
    channel STRING,
    name STRING,
    score double,
    new_score as score + 0.1
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://10.201.0.205:3306/test?charset=utf8',
   'table-name' = 'xxx_dim_mysql',
   'username' = 'root',
   'password' = '123456',
   'lookup.cache.max-rows' = '1000',
   'lookup.cache.ttl' = '10 sec'
 );
 

CREATE TABLE print_table (
  tableName string,
  new_score double
) WITH (
  'connector' = 'print'
);

insert into print_table select s1.tableName, sum(d.new_score) as new_score from dct_msg s1 left join MyDimMysql FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.tableName = d.channel  
GROUP BY TUMBLE(s1.proctime, INTERVAL '1' SECONDS), s1.tableName;

/*
 where cast(JSON_VALUE(s1.msg, '$.header.readTime') as bigint) > 0
 */