CREATE TABLE IF NOT EXISTS open_mapping.dlink_default.datagen_source (
    id    int,
    a1    STRING,
    a2    STRING,
    a3    STRING,
    a4    STRING,
    a5    STRING,
    a6    STRING,
    a7    STRING,
    a8    STRING,
    a9    STRING,
    b    int,
    c    timestamp
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100000',
    'fields.id.min' = '1',
    'fields.id.max' = '100000000'
);

CREATE TABLE IF NOT EXISTS linkhouse.open_cq.test_merge(
    id    int,
    a1    STRING,
    a2    STRING,
    a3    STRING,
    a4    STRING,
    a5    STRING,
    a6    STRING,
    a7    STRING,
    a8    STRING,
    a9    STRING,
    b    int,
    c    timestamp,
    primary key(id)  NOT ENFORCED
)
WITH ( 
    'format-version' = '2', 
    'write.upsert.enabled' = 'true',
    'table_type' = 'iceberg',
    'flink.auto.rewrite.enabled' = 'true',
    'flink.rewrite.target.file.size' = '3145728',
    'flink.auto.rewrite.min-snaps-nums' = '3'
);

insert into linkhouse.open_cq.test_merge select id, a1, a2, a3, a4, a5, a6, a7, a8, a9, b, c from open_mapping.dlink_default.datagen_source;