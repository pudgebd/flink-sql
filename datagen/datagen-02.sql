CREATE TABLE datagen(
     f_sequence INT, 
     f_binary string, 
     f_random INT
) WITH ( 
     'fields.f_sequence.end' = '100', 
     'number-of-rows' = '200000', 
     'connector' = 'datagen', 
     'fields.f_sequence.start' = '1', 
     'rows-per-second' = '100', 
     'fields.f_sequence.kind' = 'sequence'
);