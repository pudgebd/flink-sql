CREATE TABLE datagen_source (
    a    STRING,
    b    STRING,
    c    STRING,
    ts as proctime()
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '1',
 'fields.a.length'='3',
 'fields.b.length'='3',
 'fields.c.length'='3'
);


CREATE TABLE print_table(
    a    STRING,
    b    STRING,
    c    STRING,
    ts   timestamp
) WITH ('connector' = 'print');

insert into print_table select * from datagen_source;