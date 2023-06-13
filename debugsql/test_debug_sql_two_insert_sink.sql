SET next.table.enable.debug.src=true;
CREATE TABLE MyKafkaSrc(
    channel STRING,
    pv STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'mytopic04',
 'properties.bootstrap.servers' = '192.168.2.201:9092',
 'properties.group.id' = 'testGroup01',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);


CREATE TABLE MyDimTable(
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '1min'
 );


set next.table.enable.debug.sink=true;
CREATE TABLE MyTestInsert(
    channel STRING,
    pv STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_test_insert',
   'username' = 'stream_dev',
   'password' = 'stream_dev'
 );
 
 
 set next.table.enable.debug.sink=true;
 CREATE TABLE MyTestInsert2(
    channel STRING,
    pv STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_test_insert_2',
   'username' = 'stream_dev',
   'password' = 'stream_dev'
 );



insert into MyTestInsert select k.channel, k.pv, d.name from MyKafkaSrc k left join MyDimTable FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel;

insert into MyTestInsert2 select k.channel, k.pv, '' as name from MyKafkaSrc k;
