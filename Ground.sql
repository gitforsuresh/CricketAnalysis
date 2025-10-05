CREATE EXTENSION if not exists tablefunc schema report_tables;

create or replace view report_tables.ground_stats_per_match as 
select * from 
(select year,id, innings, venue, concat(batting_team, ' vs ', bowling_team) as 
teams, scored_by_teams as "1st_innings", 
lead(scored_by_teams) over(partition by id order by innings) as "2nd_innings", "1st_innings_wicket",
lead("1st_innings_wicket") over(partition by id order by innings) as "2nd_innings_wicket" ,  
 "1st_innings_overs",lead("1st_innings_overs") over(partition by id order by innings) as "2nd_innings_overs",
"1st_innings_balls",
lead("1st_innings_balls") over(partition by id order by innings) as "2nd_innings_balls" ,
concat(match_won_by, ' ', win_outcome) as match_results,Player_of_match
from (select year,id, sum(runs_batter) + sum(runs_extras) as scored_by_teams, venue, 
	  max(team_wicket) as "1st_innings_wicket",max(ball_no)as "1st_innings_overs",
	  sum(valid_ball) as "1st_innings_balls",
innings, t.team as batting_team, tb.team as bowling_team ,
win_outcome, mt.team as match_won_by,dp.player as Player_of_match from gold.ipl_fact_table ft
left join gold.dim_venue v on ft.venue_id = v.venue_id
left join gold.dim_team t on ft.batting_team_id = t.team_id 
left join gold.dim_team tb on ft.bowling_team_id = tb.team_id 
left join gold.dim_team mt on ft.mwb_id = mt.team_id 
left join gold.dim_players dp on ft.pom_id = dp.player_id where innings in (1,2) 
group by year,v.venue_id,id,ft.venue_id,batting_team,bowling_team,win_outcome,mt.team
,venue,innings,dp.player order by venue asc, id asc, innings asc)order by venue asc, id asc, innings asc) 
;

create or replace view report_tables.wicket_kind as 
select id, innings,v.venue,wicket_kind,count(wicket_kind_id) as total from gold.ipl_fact_table ft
left join gold.dim_wicket_kind wk on ft.wicket_kind_id = wk.wicket_type_id
left join gold.dim_venue v on ft.venue_id = v.venue_id where wicket_kind is not null and 
innings in (1,2) and player_out_id is not null and venue like '%Wank%'
group by v.venue,id,innings, wicket_kind order by v.venue ,id, innings;

create or replace view report_tables.wicket_kind_pivoted as (
with cte1 as (
select * from report_tables.crosstab(
$$
select id, innings,venue, wicket_kind, total from report_tables.wicket_kind  where innings = 1
	order by id, innings, venue	
$$,
$$
select distinct(wicket_kind) from report_tables.wicket_kind order by wicket_kind
$$
	
)
as ct(id int, innings int,venue text,bowled text, caught text, "caught and bowled" text, "hit wicket" text,
	 lbw text,"obstructing the field" text, "retired hurt" text, "retired out" text, "run out" text,
	 stumped text)),
cte2 as (
	select * from report_tables.crosstab(
$$
select id, innings,venue, wicket_kind, total from report_tables.wicket_kind  where innings = 2
	order by id, innings, venue	
$$,
$$
select distinct(wicket_kind) from report_tables.wicket_kind order by wicket_kind
$$
	
)
as ct(id int, innings int,venue text,bowled text, caught text, "caught and bowled" text, "hit wicket" text,
	 lbw text,"obstructing the field" text, "retired hurt" text, "retired out" text, "run out" text,
	 stumped text))

select * from cte1
union all
select * from cte2
order by id, innings);

create table report_tables.master_ground_stats_table as 
with cte1 as(
select id,innings, venue_id, count(runs_batter) as "4S" from gold.ipl_fact_table where runs_batter = 4 and innings in (1,2) 
and runs_not_boundary != 1
group by id,venue_id,innings order by id, innings,"4S" desc),
cte2 as(
select id,innings, venue_id, count(runs_batter) as "6S" from gold.ipl_fact_table where runs_batter = 6 and innings in (1,2) 
and runs_not_boundary != 1
group by id,venue_id,innings order by id, innings,"6S" desc),
cte3 as (
select id,innings, venue_id, sum(runs_batter) + sum(runs_extras) as "0 to 6 overs" from gold.ipl_fact_table where ball_no between 
0.1 and 5.6 and innings in (1,2) 
group by id,venue_id,innings order by id, innings,"0 to 6 overs" desc),
cte4 as (
select id,innings, venue_id, sum(runs_batter) + sum(runs_extras) as "6 to 10 overs" from gold.ipl_fact_table where ball_no between 
6.1 and 9.6 and innings in (1,2) 
group by id,venue_id,innings order by id, innings,"6 to 10 overs" desc),
cte5 as (
select id,innings, venue_id, sum(runs_batter) + sum(runs_extras) as "10 to 15 overs" from gold.ipl_fact_table where ball_no between 
10.1 and 14.6 and innings in (1,2) 
group by id,venue_id,innings order by id, innings,"10 to 15 overs" desc),
cte6 as (
select id,innings, venue_id, sum(runs_batter) + sum(runs_extras) as "15 to 20 overs" from gold.ipl_fact_table where ball_no between 
15.1 and 19.6 and innings in (1,2) 
group by id,venue_id,innings order by id, innings,"15 to 20 overs" desc )

select gs.id, gs.year, gs.venue, gs.teams, "1st_innings", "2nd_innings","1st_innings_wicket","2nd_innings_wicket",
"1st_innings_overs", "2nd_innings_overs","1st_innings_balls","2nd_innings_balls",c1."4S",c2."6S",
c3."0 to 6 overs", c4."6 to 10 overs",c5."10 to 15 overs",c6."15 to 20 overs",
bowled , caught , "caught and bowled" , "hit wicket" ,
lbw ,"obstructing the field" , "retired hurt" , "retired out" , "run out" ,
stumped,match_results,player_of_match
from report_tables.ground_stats_per_match gs
left join cte1 c1 on gs.id = c1.id and gs.innings = c1.innings
left join cte2 c2 on gs.id = c2.id and gs.innings = c2.innings
left join cte3 c3 on gs.id = c3.id and gs.innings = c3.innings
left join cte4 c4 on gs.id = c4.id and gs.innings = c4.innings
left join cte5 c5 on gs.id = c5.id and gs.innings = c5.innings
left join cte6 c6 on gs.id = c6.id and gs.innings = c6.innings
left join report_tables.wicket_kind_pivoted wk on gs.id = wk.id and gs.innings = wk.innings;


--
create table  report_tables.normal_ground_stats  as 
select id, year, venue, teams, "1st_innings","2nd_innings", "1st_innings_wicket","2nd_innings_wicket"
,"1st_innings_overs","2nd_innings_overs", match_results,player_of_match 
from report_tables.master_ground_stats_table where innings = 1;

update report_tables.normal_ground_stats set "1st_innings_overs" = 20 
where "1st_innings_overs" = 19.6;

update report_tables.normal_ground_stats set "2nd_innings_overs" = 20 
where "2nd_innings_overs" = 19.6;


