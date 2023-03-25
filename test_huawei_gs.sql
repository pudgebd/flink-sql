CREATE TABLE MyKafkaSrc01(
    insert_id BIGINT,
    channel STRING,
    pv STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = '你的kafka地址',
 'properties.group.id' = 'testGroup02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);

CREATE TABLE print_table(
    channel STRING,
    name STRING
) WITH ('connector' = 'print');

/**
CREATE CATALOG mypg WITH(
    'type' = 'jdbc',
    'default-database' = '你的库名',
    'username' = '你的用户名',
    'password' = '你的密码',
    'base-url' = 'jdbc:postgresql://数据库ip:8000/'
);


CREATE TABLE `mypg`.`mydb`.test_huaiwei_gs (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'table-name' = '你的表名',
   'lookup.cache.max-rows' = '500000',
   'lookup.cache.ttl' = '60min'
 );
 
insert into print_table select k.channel, d.name from MyKafkaSrc01 k left join `mypg`.`mydb`.test_huaiwei_gs FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel 
GROUP BY TUMBLE(k.proctime, INTERVAL '5' SECONDS), k.channel, d.name;

*/
 
 CREATE TABLE test_huaiwei_gs (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:postgresql://数据库ip:8000/你的库名',
   'table-name' = '你的表名',
   'username' = '你的用户名',
   'password' = '你的密码',
   'lookup.cache.max-rows' = '500000',
   'lookup.cache.ttl' = '60min'
 );


insert into print_table select k.channel, d.name from MyKafkaSrc01 k left join test_huaiwei_gs FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel 
GROUP BY TUMBLE(k.proctime, INTERVAL '5' SECONDS), k.channel, d.name;

