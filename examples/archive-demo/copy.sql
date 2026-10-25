INSERT INTO FILE avro('output')
SELECT * FROM FILE avro('input') WITH (GROUP = 'archive-demo');
