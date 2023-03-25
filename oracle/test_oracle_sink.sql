CREATE TABLE MyKafkaSrc01(
    id bigint,
    name STRING,
    age boolean,
    birthday timestamp,
    score double,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup01',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);


CREATE TABLE TEST_ORACLE_SINK(
    id bigint,
    name STRING,
    age boolean,
    birthday timestamp,
    score double
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:oracle:thin:@10.201.0.77:1521:helowin',
   'table-name' = 'MIKE.TEST_ORACLE_SINK', -- 不支持直接指定 database
   'username' = 'mike',
   'password' = 'yca1cahk'
 );
-- score double
-- 'lookup.cache.max-rows' = '10000',
-- 'lookup.cache.ttl' = '10 sec'


insert into TEST_ORACLE_SINK select id, name, age, birthday, score from MyKafkaSrc01



