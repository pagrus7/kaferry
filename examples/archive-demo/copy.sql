INSERT INTO ARCHIVE 'output'
SELECT * FROM ARCHIVE 'input' WITH (GROUP = 'archive-demo');
