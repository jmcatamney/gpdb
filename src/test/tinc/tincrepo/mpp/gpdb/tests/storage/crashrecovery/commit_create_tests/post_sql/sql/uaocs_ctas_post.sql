-- @product_version gpdb: [4.3.0.0-]
\d+ cr_uaocs_ctas
select count(*) from cr_uaocs_ctas;
drop table cr_uaocs_ctas;
drop table cr_seed_table_for_uaocs;
