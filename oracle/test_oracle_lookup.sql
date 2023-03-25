CREATE TABLE MyKafkaSrc01(
    TABLE_NAME STRING,
    channel string,
    pv STRING,
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

-- WWV_FLOW_PROCESSING,a,1
-- WWV_FLOW_STEPS,b,1
-- WWV_FLOW_STEP_PROCESSING,c,1

-- 新建的 oracle 表的字段必须全部大写，flink sql中可以小写
CREATE TABLE MyDimOracle(
    TABLE_NAME STRING,
    COLUMN_NAME STRING,
    OBSOLETE_DATE timestamp
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:oracle:thin:@10.201.0.77:1521:helowin',
   'table-name' = 'MIKE.WWV_COLUMN_EXCEPTIONS', -- 不支持直接指定 database
   'username' = 'mike',
   'password' = 'yca1cahk',
   'lookup.cache.max-rows' = '1000',
   'lookup.cache.ttl' = '5 sec'
 );



CREATE TABLE print_table01(
    TABLE_NAME STRING,
    COLUMN_NAME string,
    counts BIGINT
) WITH ('connector' = 'print');


insert into print_table01 select s1.TABLE_NAME, first_value(d.COLUMN_NAME) as COLUMN_NAME, count(d.COLUMN_NAME) as counts from MyKafkaSrc01 s1 left join MyDimOracle FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.TABLE_NAME = d.TABLE_NAME 
GROUP BY TUMBLE(s1.proctime, INTERVAL '2' SECONDS), s1.TABLE_NAME;




