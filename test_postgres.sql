CREATE TABLE MyKafkaSrc02(
    channel STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc02',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup02',
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
    name STRING
) WITH ('connector' = 'print');

/*
CREATE CATALOG mypg WITH(
    'type' = 'jdbc-hz',
    'default-database' = 'mydb',
    'username' = 'postgres',
    'password' = 'postgres',
    'base-url' = 'jdbc:postgresql://localhost:5432/'
);

-- 这种写法，原生不支持 lookup.cache.max-rows
CREATE TABLE cq_dim_pg (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc-hz',
   'table-name' = 'cq_dim_pg',
   'lookup.cache.max-rows' = '3214',
   'lookup.cache.ttl' = '1min'
 );
*/


CREATE TABLE cq_dim_pg (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc-hz',
   'url' = 'jdbc:postgresql://localhost:5432/mydb',
   'table-name' = 'cq_dim_pg',
   'username' = 'postgres',
   'password' = 'postgres',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '1min'
 );


-- `mypg`.`mydb`.
insert into print_table select k.channel, d.name from MyKafkaSrc02 k left join cq_dim_pg FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel 
GROUP BY HOP(k.proctime, INTERVAL '1' SECONDS,  INTERVAL '1' SECONDS), k.channel, d.name;

/*
MyKafkaSrc02 测试数据：
a
b
*/

/**
CREATE TABLE cq_dim_pg (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'url' = 'jdbc:postgresql://localhost:5432/mydb?charset=utf8',
   'table-name' = 'cq_dim_pg',
   'username' = 'postgres',
   'password' = 'postgres',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '10 sec'
 );



-- `mypg`.`mydb`.cq_dim_pg
insert into print_table select k.channel, d.name from MyKafkaSrc02 k left join cq_dim_pg FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel 
GROUP BY TUMBLE(k.proctime, INTERVAL '1' SECONDS), k.channel, d.name;
*/



