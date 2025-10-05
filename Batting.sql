create schema if not exists report_tables;

--views
create or replace view report_tables.total_matches as (
select count(distinct(id)) as innings_played,year,batter_id from (
select distinct(id), year,batter_id from gold.ipl_fact_table 
union
select distinct(id),year, non_striker_id from gold.ipl_fact_table 
) where batter_id is not null group by batter_id,year order by innings_played desc);

create or replace view report_tables.year_wise_matches as (
select count(distinct(id)) as innings_played,batter_id from (
select distinct(id),batter_id from gold.ipl_fact_table 
union
select distinct(id), non_striker_id from gold.ipl_fact_table 
) where batter_id is not null group by batter_id order by innings_played desc);

create or replace view report_tables.batsmen_run_record as 
select year,batter_id, sum(run_per_match) as total_runs, max(run_per_match) as highest_score from 
(select year,id, batter_id, sum(runs_batter) as run_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, batter_id,year
 order by year asc,run_per_match desc)
group by batter_id,year order by year desc,total_runs desc;

create or replace view report_tables.fours as 
select year,batter_id, count(runs_batter) as "4S" from gold.ipl_fact_table where runs_batter = 4 and innings in (1,2) 
and runs_not_boundary != 1
group by batter_id,year order by year desc, "4S" desc;

create or replace view report_tables.sixes as 
select year,batter_id, count(runs_batter) as "6S" from gold.ipl_fact_table where runs_batter = 6 and innings in (1,2) 
and runs_not_boundary != 1
group by batter_id,year order by year desc,"6S" desc;

create or replace view report_tables.thirtees as 
select year,batter_id, count(runs_per_match) as "30s" from (select year,id, batter_id, sum(runs_batter) as runs_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, year,batter_id
order by runs_per_match desc) where runs_per_match >= 30 and runs_per_match < 50 group by batter_id,year
order by year desc,"30s" desc;

create or replace view report_tables.fiftees as 
select year,batter_id, count(runs_per_match) as "50s" from (select year,id, batter_id, sum(runs_batter) as runs_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, year,batter_id
order by runs_per_match desc) where runs_per_match >= 50 and runs_per_match < 100 group by batter_id,year
order by year desc,"50s" desc;

create or replace view report_tables.hundreds as 
select year,batter_id, count(runs_per_match) as "100s" from (select year,id, batter_id, sum(runs_batter) as runs_per_match from gold.ipl_fact_table
where innings in (1,2) group by id,year, batter_id
order by runs_per_match desc) where runs_per_match >= 100 group by batter_id,year
order by year desc,"100s" desc;

create or replace view report_tables.total_balls as 
select year,batter_id, sum(balls_faced) as total_balls_faced from gold.ipl_fact_table  where innings in (1,2)
group by batter_id,year order by year desc,total_balls_faced desc;

create or replace view report_tables.Innings_played as 
select year,batter_id, count(distinct(id)) as innings_played from (
select distinct(id), year,batter_id from gold.ipl_fact_table 
union
select distinct(id), year,non_striker_id from gold.ipl_fact_table 
) group by batter_id,year order by year desc, innings_played desc;

create or replace view report_tables.notouts as 
select ft.year,tm.batter_id, innings_played - count(distinct(id)) as not_outs from gold.ipl_fact_table ft
right join report_tables.total_matches tm on tm.batter_id = ft.player_out_id and tm.year = ft.year
where player_out_id is not null
group by  ft.year,innings_played,tm.batter_id order by  year desc;


--Partnership Statistics
create table report_tables.partnership_data as (
select id, batting_partners, sum(runs_batter) + sum(runs_extras) as partnership_runs, v.venue
from gold.ipl_fact_table ft left join gold.dim_batting_partners on partner_id = partnership_id
left join gold.dim_venue v on ft.venue_id = v.venue_id
group by batting_partners, id, v.venue order by partnership_runs desc);


--batsmen statistics(who played an innings)

create table report_tables.batting_records_per_player as 
with cte1 as (
select batter_id, sum(run_per_match) as total_runs, max(run_per_match) as highest_score from 
(select id, batter_id, sum(runs_batter) as run_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, batter_id 
order by run_per_match desc)
group by batter_id order by total_runs desc),
cte2 as (
select batter_id, count(runs_batter) as "4S" from gold.ipl_fact_table where runs_batter = 4 and innings in (1,2) 
and runs_not_boundary != 1
group by batter_id order by "4S" desc),
cte3 as(
select batter_id, count(runs_batter) as "6S" from gold.ipl_fact_table where runs_batter = 6 and innings in (1,2) 
and runs_not_boundary != 1
group by batter_id order by "6S" desc),
cte4 as (
select batter_id, count(runs_per_match) as "30s" from (select id, batter_id, sum(runs_batter) as runs_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, batter_id
order by runs_per_match desc) where runs_per_match >= 30 and runs_per_match < 50 group by batter_id
order by "30s" desc),
cte5 as (
select batter_id, count(runs_per_match) as "50s" from (select id, batter_id, sum(runs_batter) as runs_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, batter_id
order by runs_per_match desc) where runs_per_match >= 50 and runs_per_match < 100 group by batter_id
order by "50s" desc),
cte6 as (
select batter_id, count(runs_per_match) as "100s" from (select id, batter_id, sum(runs_batter) as runs_per_match from gold.ipl_fact_table
where innings in (1,2) group by id, batter_id
order by runs_per_match desc) where runs_per_match >= 100 group by batter_id
order by "100s" desc),
cte7 as (select batter_id, sum(balls_faced) as total_balls_faced from gold.ipl_fact_table  where innings in (1,2)
group by batter_id order by total_balls_faced desc),
cte8 as (select batter_id, count(distinct(id)) as innings_played from (
select distinct(id), batter_id from gold.ipl_fact_table 
union
select distinct(id), non_striker_id from gold.ipl_fact_table 
) group by batter_id order by innings_played desc),
cte9 as (select tm.batter_id, innings_played - count(distinct(id)) as not_outs from gold.ipl_fact_table ft
right join report_tables.year_wise_matches tm on tm.batter_id = ft.player_out_id
where player_out_id is not null
group by innings_played,tm.batter_id order by count(distinct(id)) desc)

select dm.player, cte8.innings_played, cte9.not_outs, cte1.total_runs, cte1.highest_score,
round((cte1.total_runs/(cte8.innings_played - cte9.not_outs)), 2)as  average, 
coalesce(cte4."30s",0) as "30s",
coalesce(cte5."50s",0) as "50s",coalesce(cte6."100s",0) as "100s",
coalesce(cte2."4S",0) as "4s", coalesce(cte3."6S",0) as "6s", cte7.total_balls_faced,
round(((cte1.total_runs/cte7.total_balls_faced) * 100),2) as strike_rate
from cte1
left join cte2 on cte1.batter_id = cte2.batter_id
left join cte4 on cte1.batter_id = cte4.batter_id
left join cte5 on cte1.batter_id = cte5.batter_id
left join cte6 on cte1.batter_id = cte6.batter_id
left join cte3 on cte1.batter_id = cte3.batter_id
left join cte7 on cte1.batter_id = cte7.batter_id
left join cte8 on cte1.batter_id = cte8.batter_id
left join cte9 on cte1.batter_id = cte9.batter_id
left join gold.dim_players dm on cte1.batter_id = dm.player_id
order by cte8.innings_played desc;


create MATERIALIZED VIEW  report_tables.bat_records_per_year as 
select bt.year, dm.player,coalesce(ip.innings_played,0) as innings_played,
bt.total_runs, bt.highest_score, 
coalesce(round((bt.total_runs/(ip.innings_played - nos.not_outs)), 2),'0') as  average,
coalesce(fos."4S",0) as "4s" ,
coalesce(six."6S",0) as "6s",coalesce(thi."30s",0) as "30s",coalesce(fif."50s",0) as "50s"
,coalesce(hun."100s",0) as "100s",coalesce(tob.total_balls_faced,0) as total_balls_faced,
coalesce(round(((bt.total_runs/tob.total_balls_faced) * 100),2),0) as strike_rate,
coalesce(nos.not_outs,0) as not_outs, rank() over(partition by bt.year order by total_runs desc) as rank
from report_tables.batsmen_run_record bt
left join report_tables.fours fos on bt.year = fos.year and bt.batter_id = fos.batter_id
left join report_tables.sixes six on bt.year = six.year and bt.batter_id = six.batter_id
left join report_tables.thirtees thi on bt.year = thi.year and bt.batter_id = thi.batter_id
left join report_tables.fiftees fif on bt.year = fif.year and bt.batter_id = fif.batter_id
left join report_tables.hundreds hun on bt.year = hun.year and bt.batter_id = hun.batter_id
left join report_tables.total_balls tob on bt.year = tob.year and bt.batter_id = tob.batter_id
left join report_tables.Innings_played ip on bt.year = ip.year and bt.batter_id = ip.batter_id
left join report_tables.notouts nos on bt.year = nos.year and bt.batter_id = nos.batter_id
left join gold.dim_players dm on bt.batter_id = dm.player_id
order by year desc, total_runs desc;

DO $$
DECLARE
    min_year INT;
    max_year INT;
    current_year INT;
    table_name TEXT;
	schema_name_1 text := 'report_tables';
    create_table_sql TEXT;
BEGIN
    SELECT min(year), max(year)
	into min_year, max_year
    FROM report_tables.bat_records_per_year;

    FOR current_year IN REVERSE max_year..min_year LOOP
        table_name := 'Top_batting_' || current_year;
        create_table_sql := format('CREATE TABLE %I.%I as 
select * from report_tables.bat_records_per_year 
where year = %s and rank between 1 and 50',schema_name_1,table_name, current_year);

        EXECUTE create_table_sql;
        RAISE NOTICE 'Table % created.', table_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;