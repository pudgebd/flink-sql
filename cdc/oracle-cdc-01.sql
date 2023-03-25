CREATE TABLE cdc_table (
    ID bigint,
    NAME string
) WITH (
    'connector' = 'oracle-cdc',
    'hostname' = '10.201.0.77',
    'port' = '1521',
    'username' = 'flinkuser',
    'password' = 'flinkpw',
    'database-name' = 'helowin',
    'schema-name' = 'CDC_TEST',
    'table-name' = 'CDC_TEST.T1',
    'scan.startup.mode' = 'initial',
    'debezium.database.tablename.case.insensitive' = 'false',
    'debezium.log.mining.strategy' = 'online_catalog',
    'debezium.log.mining.continuous.mine' = 'true'
);

CREATE TABLE print_table (
    ID bigint,
    NAME string
) WITH (
    'connector' = 'print'
);


insert into print_table select * from cdc_table;
