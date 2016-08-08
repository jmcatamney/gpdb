-- @description : delete from UAO tables and Analyze after first Insert only
-- 

-- Create AO tables
DROP TABLE IF EXISTS sto_uao_city_analyze_first_del cascade;
BEGIN;

CREATE TABLE sto_uao_city_analyze_first_del (
    id integer NOT NULL,
    name text NOT NULL,
    countrycode character(3) NOT NULL,
    district text NOT NULL,
    population integer NOT NULL
) with (appendonly=true) distributed by(id);

-- gucs set to auto delete from statistics after first  insert
set gp_autostats_mode=ON_NO_STATS;

select count(*)  from sto_uao_city_analyze_first_del;
select relname, reltuples from pg_class where relname = 'sto_uao_city_analyze_first_del';
SELECT 1 AS VisimapPresent  FROM pg_appendonly WHERE visimapidxid is not NULL AND visimapidxid is not NULL AND relid=(SELECT oid FROM pg_class WHERE relname='sto_uao_city_analyze_first_del');

-- Copy 7 rows in table sto_uao_city_analyze_first_del
COPY sto_uao_city_analyze_first_del (id, name, countrycode, district, population) FROM stdin;
1	Kabul	AFG	Kabol	1780000
2	Qandahar	AFG	Qandahar	237500
3	Herat	AFG	Herat	186800
4	Mazar-e-Sharif	AFG	Balkh	127800
5	Amsterdam	NLD	Noord-Holland	731200
6	Rotterdam	NLD	Zuid-Holland	593321
7	Haag	NLD	Zuid-Holland	440900
\.

COMMIT;

select count(*)  from sto_uao_city_analyze_first_del;
select relname, reltuples from pg_class where relname = 'sto_uao_city_analyze_first_del';

select *  from sto_uao_city_analyze_first_del order by id;
-- Increase the population by 1000 for all cities in NetherLands (NLD) 
-- Should delete from 3 rows
delete from sto_uao_city_analyze_first_del where countrycode='NLD';
select count(*)  AS only_visi_tups from sto_uao_city_analyze_first_del;
set gp_select_invisible = true;
select count(*)  AS invisi_and_visi_tups from sto_uao_city_analyze_first_del;
set gp_select_invisible = false;
select relname, reltuples from pg_class where relname = 'sto_uao_city_analyze_first_del';

