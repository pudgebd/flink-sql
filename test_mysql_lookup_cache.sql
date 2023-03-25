CREATE TABLE MyKafkaSrc01(
    channel STRING,
    name string,
    pv STRING,
    proctime as PROCTIME()
)
WITH (
 'connector' = 'kafka',
 'topic' = 'MyKafkaSrc01',
 'properties.bootstrap.servers' = 'cdh601:9092,cdh602:9092,cdh603:9092',
 'properties.group.id' = 'testGroup01',
 'format' = 'csv',
 'scan.startup.mode' = 'latest-offset',
 'csv.ignore-parse-errors' = 'true',
 'properties.security.protocol' = 'SASL_PLAINTEXT',
 'properties.sasl.mechanism' = 'GSSAPI',
 'properties.sasl.kerberos.service.name' = 'kafka',
 'properties.sasl.jaas.config' = 'com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true refreshKrb5Config=true storeKey=true serviceName=kafka keyTab="/home/work/work.keytab" principal="work@xxx.COM";'
);


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


-- 有个id主键，此处可不定义
CREATE TABLE `default_catalog`.`default_database`.MyDimTable(
    channel STRING,
    name STRING,
    score double
 )WITH(
   'connector' = 'jdbc-hz',
   'url' = 'jdbc:mysql://192.168.1.59:3306/stream_dev?charset=utf8',
   'table-name' = 'cq_dim_mysql',
   'username' = 'stream_dev',
   'password' = 'stream_dev',
   'lookup.cache.max-rows' = 'all',
   'lookup.cache.ttl' = '10 sec'
 );


CREATE TABLE print_table01(
    channel STRING,
    name STRING,
    score double
) WITH ('connector' = 'print');

CREATE TABLE print_table02(
    channel STRING,
    score double
) WITH ('connector' = 'print');

/*
-- CREATE FUNCTION myDemoUdf AS 'pers.pudgebd.flink.udf.MyDemoUdf' LANGUAGE JAVA
package org.apache.
package com.xxx.streamx.

import org.apache.
import com.xxx.streamx.

*/

/*
insert into print_table01 select s1.channel, d.name, sum(d.score) as score from MyKafkaSrc01 s1 left join `default_catalog`.`default_database`.MyDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel and s1.name = d.name 
GROUP BY TUMBLE(s1.proctime, INTERVAL '1' SECONDS), s1.channel, d.name;
*/

insert into print_table02 select s1.channel, sum(d.score) as score from MyKafkaSrc01 s1 left join MyDimTable FOR SYSTEM_TIME AS OF s1.proctime AS d on s1.channel = d.channel 
GROUP BY TUMBLE(s1.proctime, INTERVAL '1' SECONDS), s1.channel;




