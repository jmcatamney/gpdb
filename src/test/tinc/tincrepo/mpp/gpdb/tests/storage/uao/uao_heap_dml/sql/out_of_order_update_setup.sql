-- start_ignore
SET gp_create_table_random_default_distribution=off;
-- end_ignore
DROP TABLE IF EXISTS foo;
DROP TABLE IF EXISTS bar;

CREATE TABLE foo (a INT, b INT, c CHAR(100)) ;
INSERT INTO foo SELECT i as a, i as b, '' as c FROM generate_series(1, 100000) AS i;
INSERT INTO foo SELECT i as a, i as b, '' as c FROM generate_series(1, 100000) AS i;
CREATE TABLE bar (a INT, c CHAR(100));
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
INSERT INTO bar SELECT i as a, '' as c  FROM generate_series(1, 100000) AS i;
ANALYZE foo;
ANALYZE bar;

