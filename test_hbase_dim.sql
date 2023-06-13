/**
1.11 hbase 不能和 event time WATERMARK 的表 FOR SYSTEM_TIME AS OF
CREATE TABLE MyKafkaSrc02(
    channel STRING,
    pv STRING,
    ts bigint,
    ts_timestamp as bigint_to_ts(ts),
    WATERMARK FOR ts_timestamp AS ts_timestamp - INTERVAL '5' SECONDS
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);
*/

CREATE TABLE MyKafkaSrc02(
    channel STRING,
    pv STRING,
    proctime AS PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true'
);

 CREATE TABLE MyDimTable(
    channel STRING,
    cf ROW<name STRING>
 )WITH(
   'connector' = 'hbase-1.4',
   'table-name' = 'hbase_dim:xxx_dim_channel_name',
   'zookeeper.quorum' = 'localhost:2181'
 );

CREATE TABLE print_table01 (
    channel STRING,
    pv STRING,
    name STRING
) WITH ('connector' = 'print');

CREATE TABLE print_table02 (
    channel STRING,
    pv STRING
) WITH ('connector' = 'print');

/**
CREATE FUNCTION myDemoUdf AS 'pers.pudgebd.flink.udf.MyDemoUdf' LANGUAGE JAVA USING JAR 'hdfs://cdh601:8020/user/xxx/udf/jars/flink.udf-1.0-jar-with-dependencies.jar';
*/


insert into print_table01 select k.channel, k.pv, d.name from MyKafkaSrc02 k left join MyDimTable FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel;

/**
insert into print_table02 select k.channel, k.pv from MyKafkaSrc02 k;
*/