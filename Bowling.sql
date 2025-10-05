--views

create or replace view report_tables.bowler_stats as 
select year,bowler_id, sum(bowler_wicket) as total_wickets,
count(distinct(id))as innings_bowled,sum(runs_bowler) as total_runs,
sum(valid_ball) as total_balls, SUM(valid_ball)/6 as total_overs,
 round((SUM(runs_bowler) / (SUM(valid_ball) / 6.0)),2) AS Economy,
 round((sum(runs_bowler) / NULLIF(sum(bowler_wicket), 0.0)),2) as average,
round(sum(valid_ball)/nullif(sum(bowler_wicket),0.0),2) as strike_rate
 from 
gold.ipl_fact_table  where innings in (1,2) and bowler_id = 381
group by year,bowler_id order by year desc ,total_wickets desc;

create or replace view report_tables.threes as 
select year,bowler_id, count(wicket_per_match) as "3s" from (select year,id, bowler_id, 
sum(bowler_wicket) as wicket_per_match from gold.ipl_fact_table where innings in (1,2)
group by year,id, bowler_id having sum(bowler_wicket) >=3) where bowler_id = 381
group by year,bowler_id order by year desc, "3s" desc, bowler_id desc;

create or replace view report_tables.fours_wick as 
select year,bowler_id, count(wicket_per_match) as "4s" from (select year,id, bowler_id, 
sum(bowler_wicket) as wicket_per_match from gold.ipl_fact_table where innings in (1,2)
group by year,id, bowler_id having sum(bowler_wicket) >=4)
group by year, bowler_id order by year desc,"4s" desc;

create or replace view report_tables.fives as 
select year,bowler_id, count(wicket_per_match) as "5s" from (select year, id, bowler_id, 
sum(bowler_wicket) as wicket_per_match from gold.ipl_fact_table where innings in (1,2)
group by year,id, bowler_id having sum(bowler_wicket) >=5)
group by year,bowler_id order by year desc,"5s" desc;

create or replace view report_tables.bests as 
select year,bowler_id, best_figures from (select year,bowler_id, best_figures, tw, rb, 
row_number() over(partition by year,bowler_id order by tw desc) as rank from (select year,id, bowler_id,sum(bowler_wicket) as tw, sum(runs_bowler) as rb,
concat(sum(bowler_wicket), '/', sum(runs_bowler)) as best_figures
from gold.ipl_fact_table where innings in (1,2) group by id, year,
bowler_id)) where rank = 1 order by year desc,bowler_id desc;

--Bowlers statistics (who bowled at least 1 ball in the IPL)

create table report_tables.bowling_records_per_player as 
with cte1 as (
(select bowler_id, sum(bowler_wicket) as total_wickets,
count(distinct(id))as innings_bowled,sum(runs_bowler) as total_runs,
sum(valid_ball) as total_balls, SUM(valid_ball)/6 as total_overs,
 round((SUM(runs_bowler) / (SUM(valid_ball) / 6.0)),2) AS Economy,
 round((sum(runs_bowler) / NULLIF(sum(bowler_wicket), 0.0)),2) as average,
 round(sum(valid_ball)/nullif(sum(bowler_wicket),0.0),2) as strike_rate
 from 
gold.ipl_fact_table  where innings in (1,2)
group by bowler_id order by total_wickets desc )),
cte2 as (
select bowler_id, count(wicket_per_match) as "3s" from (select id, bowler_id, 
sum(bowler_wicket) as wicket_per_match from gold.ipl_fact_table where innings in (1,2)
group by id, bowler_id having sum(bowler_wicket) >=3)
group by bowler_id order by "3s" desc),
cte3 as 
(select bowler_id, count(wicket_per_match) as "4s" from (select id, bowler_id, 
sum(bowler_wicket) as wicket_per_match from gold.ipl_fact_table where innings in (1,2)
group by id, bowler_id having sum(bowler_wicket) >=4)
group by bowler_id order by "4s" desc),
cte4 as
(select bowler_id, count(wicket_per_match) as "5s" from (select id, bowler_id, 
sum(bowler_wicket) as wicket_per_match from gold.ipl_fact_table where innings in (1,2)
group by id, bowler_id having sum(bowler_wicket) >=5)
group by bowler_id order by "5s" desc),
cte5 as (
select bowler_id, best_figures from (select bowler_id, best_figures, tw, rb, 
row_number() over(partition by bowler_id order by tw desc) as rank from (select id, bowler_id,sum(bowler_wicket) as tw, sum(runs_bowler) as rb,
concat(sum(bowler_wicket), '/', sum(runs_bowler)) as best_figures
from gold.ipl_fact_table where innings in (1,2) group by id, 
bowler_id)) where rank = 1)

select dm.player,cte1.total_wickets, cte1.innings_bowled,cte1.total_runs,cte1.total_overs,
cte1.Economy, cte1.average,coalesce(cte2."3s",0) as "3s",coalesce(cte3."4s",0) as "4s",
coalesce(cte4."5s",0) as "5s",cte5.best_figures, cte1.strike_rate from cte1
left join cte2 on cte1.bowler_id = cte2.bowler_id
left join cte3 on cte1.bowler_id = cte3.bowler_id
left join cte4 on cte1.bowler_id = cte4.bowler_id
left join cte5 on cte1.bowler_id = cte5.bowler_id
left join gold.dim_players dm on cte1.bowler_id = dm.player_id
order by total_wickets desc;

create materialized view report_tables.bowling_records_per_year as 
select bs.year, dm.player, bs.total_wickets, bs.innings_bowled,bs.total_runs,bs.total_balls,bs.total_overs,coalesce(th."3s",0) as "3s",coalesce(fw."4s",0) as "4s",
coalesce(fv."5s",0) as "5s",bt.best_figures,bs.Economy, bs.average,bs.strike_rate,
rank() over(partition by bt.year order by total_wickets desc) as rank
from report_tables.bowler_stats bs 
left join report_tables.threes th on bs.year = th.year and bs.bowler_id = th.bowler_id
left join report_tables.fours_wick fw on bs.year = fw.year and bs.bowler_id = fw.bowler_id
left join report_tables.fives fv on bs.year = fv.year and bs.bowler_id = fv.bowler_id
left join report_tables.bests bt on bs.year = bt.year and bs.bowler_id = bt.bowler_id
left join gold.dim_players dm on bs.bowler_id = dm.player_id
order by year desc, total_wickets desc;

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
    FROM report_tables.bowling_records_per_year;

    FOR current_year IN REVERSE max_year..min_year LOOP
        table_name := 'Top_bowling_' || current_year;
        create_table_sql := format('CREATE TABLE %I.%I as 
select * from report_tables.bowling_records_per_year 
where year = %s and rank between 1 and 50',schema_name_1,table_name, current_year);

        EXECUTE create_table_sql;
        RAISE NOTICE 'Table % created.', table_name;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

