CREATE TABLE MyKafkaSrc01(
    channel STRING,
    name string,
    pv STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'test_mysql_9w_cache_all_topic',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'test_mysql_9w_cache_all_group',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);


-- 有个id主键，此处可不定义
CREATE TABLE MyDimTable (
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql_9w',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '1 hour'
 );


CREATE TABLE KafkaJoinAllCacheResult (
    channel STRING,
    name STRING,
    score double
) WITH (
 'connector' = 'kafka',
 'topic' = 'join_all_cache_result',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'join_all_cache_result_group',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);



insert into KafkaJoinAllCacheResult select k.channel, d.name, sum(d.score) as score from MyKafkaSrc01 k left join MyDimTable FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel and k.name = d.name 
GROUP BY TUMBLE(k.proctime, INTERVAL '10' SECONDS), k.channel, d.name;


