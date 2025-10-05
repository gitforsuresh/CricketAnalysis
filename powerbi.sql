--batting

create or replace view report_tables.batsmen_ipl_career as 
select 'Career' as Career,player, innings_played, not_outs, 
total_runs,highest_score,average, "30s","50s","100s","4s", "6s", total_balls_faced,strike_rate 
from report_tables.batting_records_per_player 
union all
select cast(year as text), player,innings_played,not_outs,total_runs, 
highest_score, average,"30s","50s","100s","4s" ,"6s",
total_balls_faced,strike_rate from report_tables.bat_records_per_year
order by career desc,player asc;

create or replace view top_50_batsmen_per_year as 
select * from report_tables.bat_records_per_year where rank between 1 and 50;



--bowling

create or replace view report_tables.bowler_ipl_career as 
select cast(year as text), player, total_wickets, innings_bowled,total_runs,total_overs,Economy,average,"3s","4s",
"5s",best_figures,strike_rate from report_tables.bowling_records_per_year
union all
select 'Career' as Career, player,total_wickets, innings_bowled,total_runs,total_overs,
Economy, average,"3s","4s",
"5s",best_figures,strike_rate from report_tables.bowling_records_per_player;

create or replace view report_tables.bowlings_with_10_wickets as
select * from report_tables.bowling_records_per_year where total_wickets >= 10
order by year desc, rank asc;

--Ground  stats

create or replace view report_tables.stadium_match_results as 
select year, venue, teams, 
concat("1st_innings",'/',"1st_innings_wicket",' ', "1st_innings_overs",' overs') as "1st_innings",
concat("2nd_innings",'/',"2nd_innings_wicket",' ', "2nd_innings_overs",' overs') as "2nd_innings",
match_results, player_of_match
from (select * from report_tables.normal_ground_stats) where venue like '%Wank%' order by year desc;

create or replace view report_tables.hompage_data_min_2nd as 
select min("2nd_innings") as lowest_2nd_innings from report_tables.normal_ground_stats 
where player_of_match is not null and "2nd_innings_wicket" = 10;

create or replace view report_tables.hompage_data_max_2nd as 
select max("2nd_innings") as higest_run_chase from report_tables.normal_ground_stats;

create or replace view report_tables.hompage_data_max_1st as 
select max("1st_innings") as highest_target from report_tables.normal_ground_stats;

create or replace view report_tables.hompage_data_min_1st as 
select min("1st_innings") from report_tables.normal_ground_stats where player_of_match is not null ;

-- batting/bowling/team/ground

create or replace view report_tables.overall_report as (
with cte1 as (
select ft.id, ft.year,  t.team as batsmen_team, dm.player as batsmen, t.team as batting_team, t1.team as against,
v.venue from gold.ipl_fact_table ft
left join gold.dim_players dm on ft.batter_id = dm.player_id
left join gold.dim_team t on ft.batting_team_id = t.team_id
left join gold.dim_team t1 on ft.bowling_team_id = t1.team_id
left join gold.dim_venue v on ft.venue_id= v.venue_id where ft.innings in (1,2)
group by ft.id, ft.year,dm.player,batting_team,against,v.venue
union
select ft.id, ft.year,  t.team as batting_team, dm.player as batsmen, t.team as batting_team, t1.team as against,
v.venue from gold.ipl_fact_table ft
left join gold.dim_players dm on ft.non_striker_id = dm.player_id
left join gold.dim_team t on ft.batting_team_id = t.team_id
left join gold.dim_team t1 on ft.bowling_team_id = t1.team_id
left join gold.dim_venue v on ft.venue_id= v.venue_id where ft.innings in (1,2)
group by ft.id, ft.year,dm.player,batting_team,against,v.venue
order by year desc),
cte2 as (
select ft.id, ft.year,  t.team as batting_team, dm.player as batsmen, t.team as batting_team, t1.team as against,
sum(runs_batter) as runs,v.venue from gold.ipl_fact_table ft
left join gold.dim_players dm on ft.batter_id = dm.player_id
left join gold.dim_team t on ft.batting_team_id = t.team_id
left join gold.dim_team t1 on ft.bowling_team_id = t1.team_id
left join gold.dim_venue v on ft.venue_id= v.venue_id where ft.innings in (1,2)
group by ft.id, ft.year,dm.player,batting_team,against,v.venue),
cte3 as(
select ft.id, ft.year,  db.player as batsmen, dm.player as bowler, wicket_kind, fi.player as 
fielder, result_type from gold.ipl_fact_table ft
left join gold.dim_players db on ft.batter_id = db.player_id
left join gold.dim_players dm on ft.bowler_id = dm.player_id
left join gold.dim_players fi on ft.fielder_id = fi.player_id 
left join gold.dim_wicket_kind wk on ft.wicket_kind_id = wk.wicket_type_id
where striker_out = 1 and innings in (1,2)
union all
select ft.id, ft.year,  db.player as batsmen, dm.player as bowler, wicket_kind, fi.player as 
fielder, result_type from gold.ipl_fact_table ft
left join gold.dim_players db on ft.non_striker_id = db.player_id
left join gold.dim_players dm on ft.bowler_id = dm.player_id
left join gold.dim_players fi on ft.fielder_id = fi.player_id
left join gold.dim_wicket_kind wk on ft.wicket_kind_id = wk.wicket_type_id
where striker_out = 0 and wicket_kind is not null and innings in (1,2))

select cte1.id,cte1.year, cte1.batsmen, cte1.batsmen_team, coalesce(cte2.runs,0) as runs, 
cte1.against, cte1.venue, coalesce(cte3.bowler, 'not out') as bowler, cte3.wicket_kind, cte3.fielder from cte1 left join cte2 on 
cte1.id = cte2.id and cte1.year = cte2.year and cte1.batsmen = cte2.batsmen
left join cte3 on 
cte1.id = cte3.id and cte1.year = cte3.year and cte3.batsmen = cte2.batsmen
	where cte1.batsmen = 'V Kohli'
order by cte1.year desc, cte1.batsmen);

