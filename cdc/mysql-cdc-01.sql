CREATE TABLE cdc_t2(
    a string,
    b string,
    c int,
    d string,
    ts timestamp,
    PRIMARY KEY(c) NOT ENFORCED
) WITH ( 
 'connector' = 'mysql-cdc',
  'hostname' = '10.201.0.82',
  'database-name' = 'demo01',
  'table-name' = 't2',
  'username' = 'root',
  'password' = '123456',
  'scan.startup.mode' = 'initial',
  'connect.timeout' = '35s',
  'connect.max-retries' = '5',
  'scan.startup.timestamp-millis' = '1000'
);


CREATE TABLE print_table(
    a string,
    b string,
    c int,
    d string,
    ts timestamp
) WITH (
    'connector' = 'print'
);

insert into print_table select * from cdc_t2;