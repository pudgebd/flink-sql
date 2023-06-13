CREATE CATALOG myhive WITH (
    'be.hive.catalog'=true, 'hive.metastore.uris'='thrift://cdh601:9083', 'database'='default', 
    'hive.metastore.kerberos.principal.real' = 'work@xxx.COM',
    'hive.metastore.kerberos.keytab.file.path' = '/home/work/work.keytab', 
    'create.hive.catalog.timeout.seconds' = '5','java.security.krb5.conf' = '/etc/krb5.conf');


SET table.sql-dialect=default;
CREATE TABLE MyKafkaSrc01(
    channel STRING,
    goods_id INT,
    dt STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'MyKafkaSrc01',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


CREATE TABLE MyKafkaSrc02(
    goods_id INT,
    pv INT,
    order_counts INT
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'MyKafkaSrc02',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);



CREATE TABLE MyKafkaSink01(
channel STRING,
counts BIGINT,
pv INT,
order_counts INT,
name STRING,
dt STRING
)
WITH (
'connector' = 'kafka',
'topic' = 'MyKafkaSink01',
'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
'properties.group.id' = 'MyKafkaSink01',
'format' = 'csv',
'scan.startup.mode' = 'latest-offset',
'csv.ignore-parse-errors' = 'true',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


CREATE TABLE print_table(
channel STRING,
counts BIGINT,
name STRING,
dt STRING
) WITH ('connector' = 'print');

/**/
SET table.sql-dialect=hive;
CREATE TABLE `myhive`.`default`.MyHiveDimTable (
    channel STRING,
    name STRING
) PARTITIONED BY (dt String) STORED AS parquet TBLPROPERTIES (
  'connector'='filesystem',
  'path'='hdfs://cdh601:8020/user/hive/warehouse/myhivedimtable',
  'format'='parquet',
  'partition.time-extractor.timestamp-pattern'='$dt',
  'sink.partition-commit.trigger'='partition-time',
  'sink.partition-commit.delay'='1 d',
  'sink.partition-commit.policy.kind'='metastore,success-file',
  'streaming-source.enable'='true',
  'streaming-source.partition-order'='create-time',
  'streaming-source.consume-start-offset'='2020-11-10',
  'lookup.join.cache.ttl'='1 min'
);

SET table.sql-dialect=default;
CREATE TABLE `default_catalog`.`default_database`.MyDimTable (
    channel STRING,
    name STRING,    
    score double,
    dt string
 ) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'xxx_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = '50000',
   'lookup.cache.ttl' = '10 sec'
 );

-- flink 源码不支持 kerberos hdfs lookup，FOR SYSTEM_TIME AS OF s1.proctime AS 
insert into print_table 
select s1.channel, count(s1.goods_id) as counts, LAST_VALUE(d.name) as name, s1.dt from MyKafkaSrc01 s1 
left join `myhive`.`default`.MyHiveDimTable 
/*+ OPTIONS('streaming-source.monitor-interval'='30 seconds', 'streaming-source.partition.include'='all') */
 d on s1.channel = d.channel and s1.dt = d.dt 
group by s1.channel, s1.dt having sum(s1.goods_id) > 0;

/*
MyKafkaSrc01 样样例数据：
p1,1,2020-11-17

*/

/**
set next.query.sql.convert.retract.stream=true;
set next.query.retain.retract.flag=false;

insert into MyKafkaSink01 
select s1.channel, s1.goods_id, sum(s2.pv), sum(s2.order_counts), arbitrary_udaf(d.name) as name, s1.dt from MyKafkaSrc01 s1 
inner join MyKafkaSrc02 s2 on s1.goods_id = s2.goods_id 
left join `myhive`.`default`.MyHiveDimTable d on s1.channel = d.channel and s1.dt = d.dt 
group by s1.channel, s1.goods_id, s1.dt;
*/
