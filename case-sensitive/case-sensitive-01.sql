create TABLE datagen_source (
    a    STRING,
    b    STRING,
    c    STRING
) WITH (
 'connector' = 'datagen',
 'rows-per-second' = '1',
 'fields.a.length'='3',
 'fields.b.length'='3',
 'fields.c.length'='3'
);


CREATE table print_table(
    A    STRING,
    B    STRING,
    C    STRING
) with ('connector' = 'print');

insert inTO print_table select a, b, c FRom datagen_source;