-- start_ignore
SET gp_create_table_random_default_distribution=off;
-- end_ignore
CREATE TABLE sync2_hybrid_part1 (
                PS_PARTKEY INTEGER,
                PS_SUPPKEY INTEGER,
                PS_AVAILQTY integer,
                PS_SUPPLYCOST decimal,
                PS_COMMENT VARCHAR(199)
                ) 
 partition by range (ps_supplycost)
 subpartition by range (ps_suppkey)
,subpartition by range (ps_partkey) subpartition template (start('1') end('200001') every(66666)

)
(
partition p1 start('1') end('20') inclusive
(
 subpartition sp1 start('1') end('3030')  WITH (checksum=false,   appendonly=true,blocksize=1171456,compresslevel=3),
 subpartition sp2 end('6096') inclusive,
 subpartition sp3   start('6096') exclusive end('7201') with (orientation='column',appendonly=true),
 subpartition sp4 end('10001')  WITH (checksum=false,appendonly=true,     blocksize=1171456,compresslevel=3)
), 
partition p2 end('1001') inclusive
(
subpartition sp1 start('1') ,        
subpartition sp2 start('4139') with (orientation='column',appendonly=true) ,
subpartition sp3 start('4685')  WITH (checksum=false,appendonly=true, blocksize=1171456,compresslevel=3) ,
subpartition sp4 start('5675') )
);


COPY sync2_hybrid_part1 FROM '/data/tangp3/tincrepo/private/tangp3/main/storage/filerep_end_to_end/hybrid_part.data' delimiter as '|';



CREATE TABLE sync2_hybrid_part2 (
                PS_PARTKEY INTEGER,
                PS_SUPPKEY INTEGER,
                PS_AVAILQTY integer,
                PS_SUPPLYCOST decimal,
                PS_COMMENT VARCHAR(199)
                ) 
 partition by range (ps_supplycost)
 subpartition by range (ps_suppkey)
,subpartition by range (ps_partkey) subpartition template (start('1') end('200001') every(66666)

)
(
partition p1 start('1') end('20') inclusive
(
 subpartition sp1 start('1') end('3030')  WITH (checksum=false,   appendonly=true,blocksize=1171456,compresslevel=3),
 subpartition sp2 end('6096') inclusive,
 subpartition sp3   start('6096') exclusive end('7201') with (orientation='column',appendonly=true),
 subpartition sp4 end('10001')  WITH (checksum=false,appendonly=true,     blocksize=1171456,compresslevel=3)
), 
partition p2 end('1001') inclusive
(
subpartition sp1 start('1') ,        
subpartition sp2 start('4139') with (orientation='column',appendonly=true) ,
subpartition sp3 start('4685')  WITH (checksum=false,appendonly=true, blocksize=1171456,compresslevel=3) ,
subpartition sp4 start('5675') )
);


COPY sync2_hybrid_part2 FROM '/data/tangp3/tincrepo/private/tangp3/main/storage/filerep_end_to_end/hybrid_part.data' delimiter as '|';


DROP TABLE sync1_hybrid_part7;
DROP TABLE ck_sync1_hybrid_part6;
DROP TABLE ct_hybrid_part4;
DROP TABLE resync_hybrid_part2;
DROP TABLE sync2_hybrid_part1;
