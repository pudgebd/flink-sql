CREATE TABLE MyKafkaSrc01(
    id bigint,
    name string,
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


CREATE TABLE TEST_ORACLE_SINK_2(
    ID bigint,
    NAME string,
    TS_TZ timestamp,
    TS_LTZ timestamp
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:oracle:thin:@10.201.0.77:1521:helowin',
   'table-name' = 'MIKE.TEST_ORACLE_SINK_2', -- 不支持直接指定 database
   'username' = 'mike',
   'password' = 'yca1cahk'
 );

insert into TEST_ORACLE_SINK_2 select id, name, proctime, proctime from MyKafkaSrc01;


