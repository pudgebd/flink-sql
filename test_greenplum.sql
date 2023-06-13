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
/**

CREATE CATALOG mypg WITH(
    'type' = 'jdbc',
    'default-database' = 'mydb',
    'username' = 'gpadmin',
    'password' = 'gpadmin',
    'base-url' = 'jdbc:pivotal:greenplum://192.168.1.129:5432;DatabaseName=mydb'
);


CREATE TABLE `mypg`.`mydb`.xxx_dim_pg (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc',
   'table-name' = 'xxx_dim_pg',
   'lookup.cache.max-rows' = '5000',
   'lookup.cache.ttl' = '1min'
 );
*/

CREATE TABLE xxx_dim_pg (
    channel STRING,
    name STRING
 )WITH(
   'connector' = 'jdbc-hz',
   'url' = 'jdbc:pivotal:greenplum://192.168.1.129:5432;DatabaseName=mydb',
   'table-name' = 'xxx_dim_pg',
   'username' = 'gpadmin',
   'password' = 'gpadmin',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '10 sec'
 );

CREATE TABLE print_table(
    channel STRING,
    name STRING
) WITH ('connector' = 'print');

-- `mypg`.`mydb`.xxx_dim_pg
insert into print_table select k.channel, d.name from MyKafkaSrc02 k left join xxx_dim_pg FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel
GROUP BY TUMBLE(k.proctime, INTERVAL '1' SECONDS), k.channel, d.name;

/**
insert into print_table select k.channel, d.name from MyKafkaSrc02 k left join `mypg`.`mydb`.xxx_dim_pg FOR SYSTEM_TIME AS OF k.proctime AS d on k.channel = d.channel
GROUP BY HOP(k.proctime, INTERVAL '5' SECONDS,  INTERVAL '10' SECONDS), k.channel, d.name;
*/
