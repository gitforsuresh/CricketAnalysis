create schema if not exists gold;
 
drop table if exists gold.dim_match_id;
drop table if exists gold.dim_players;
drop table if exists gold.dim_team;
drop table if exists gold.dim_venue;
drop table if exists gold.dim_city;
drop table if exists gold.dim_date;
drop table if exists gold.dim_wicket_kind;
drop table if exists gold.dim_extra_type;
drop table if exists gold.dim_batting_partners;
drop table if exists gold.dim_stage;
drop table if exists gold.ipl_fact_table;
 
create table gold.dim_match_id(id serial primary key, match_id INT);
create table gold.dim_players(player_id serial primary key, player varchar);
create table gold.dim_team(team_id serial primary key, team varchar);
create table gold.dim_venue(venue_id serial primary key, venue varchar);
create table gold.dim_city(city_id serial primary key, city varchar);
create table gold.dim_date(date_id serial primary key, date DATE, year INT, 
					  day INT, month VARCHAR, day_name VARCHAR, month_name VARCHAR);
create table gold.dim_wicket_kind (wicket_type_id serial primary key, wicket_kind varchar)	;	
create table gold.dim_extra_type (extra_type_id serial primary key, extra_type varchar)	;	
create table gold.dim_batting_partners(Partner_id serial primary key , batting_partners varchar);
create table gold.dim_stage(stage_id serial primary key, stage varchar);

 
 
insert into gold.dim_match_id (match_id) (select distinct(match_id) from silver.updated_ipl_data
										 	order by match_id asc);
insert into gold.dim_players (player) (
select distinct(batter) as players from silver.updated_ipl_data
union
select distinct(non_striker) from silver.updated_ipl_data
union
select distinct(bowler) from silver.updated_ipl_data
union
select distinct(new_batter) from silver.updated_ipl_data
union
select distinct(next_batter) from silver.updated_ipl_data
union
select distinct(fielders) from silver.updated_ipl_data
order by players asc);
insert into gold.dim_team (team) (select distinct(batting_team) from silver.updated_ipl_data
								 	order by batting_team asc);
insert into gold.dim_venue (venue) (select distinct(venue) from silver.updated_ipl_data
								   order by venue asc);
insert into gold.dim_city (city) (select distinct(city) from silver.updated_ipl_data
								 order by city asc);
insert into gold.dim_wicket_kind (wicket_kind) (select distinct(wicket_kind) from silver.updated_ipl_data
											   order by wicket_kind asc);
insert into gold.dim_extra_type (extra_type) (select distinct(extra_type) from silver.updated_ipl_data
											 order by extra_type asc);
insert into gold.dim_batting_partners (batting_partners) (select distinct(batting_partners) from silver.updated_ipl_data
														 order by batting_partners asc);
insert into gold.dim_stage (stage) (select distinct(stage) from silver.updated_ipl_data
								   order by stage asc);						   
insert into gold.dim_date (date, year, day, month, day_name, month_name)
(select distinct(date), extract(year from date) as year, extract(day from date) as day,
extract(month from date) as month,
TO_CHAR(date, 'Day') as day_name, TO_CHAR(date, 'Month') as month_name 
from silver.updated_ipl_data order by date asc);


create table gold.ipl_fact_table as 
select m.id, date, innings, dt.team_id as batting_team_id, dtb.team_id as bowling_team_id, 
over,ball,ball_no,dbm.player_id as batter_id,
bat_pos, runs_batter, balls_faced, dbw.player_id as bowler_id, valid_ball, runs_extras, 
runs_total,runs_bowler,
case when runs_not_boundary = 'TRUE' then 1 else 0 end as runs_not_boundary, extra_type_id,
ns.player_id as non_striker_id, non_striker_pos, wk.wicket_type_id as wicket_kind_id, 
po.player_id as player_out_id, f.player_id as fielder_id, rb.player_id as review_batter_id,
tr.team_id as team_reviewed_id, case when review_decision = 'struck down' then 1 
when review_decision = 'upheld' then 0 else -1 end as review_decision,
case when umpires_call = 'TRUE' then 1 else 0 end as umpires_call,pom.player_id as pom_id,
mwb.team_id as mwb_id,win_outcome, 
tw.team_id as toss_winner_id,case when toss_decision = 'bat' then 1 else 0 end as toss_decision, v.venue_id, c.city_id, day, month, year,so.team_id as superover_winner_id,
case when result_type = 'no result' then 0
when result_type = 'tie' then -1 else 1 end as result_type,
case when method = 'D/L' then 1 else 0 end as win_method, st.stage_id, team_runs, team_balls,team_wicket,
nwb.player_id as new_batter_id, batter_runs, batter_balls, bowler_wicket, bp.partner_id as partnership_id, 
nxt.player_id as next_batter, 
case when striker_out = 'TRUE' then 1 else 0 end as striker_out
from silver.updated_ipl_data s
left join gold.dim_match_id m on s.match_id = m.match_id
left join gold.dim_team dt on s.batting_team = dt.team
left join gold.dim_team as dtb on s.bowling_team = dtb.team
left join gold.dim_players as dbm on s.batter = dbm.player
left join gold.dim_players as dbw on s.bowler = dbw.player
left join gold.dim_extra_type as e on s.extra_type = e.extra_type
left join gold.dim_players as ns on s.non_striker = ns.player
left join gold.dim_wicket_kind as wk on s.wicket_kind = wk.wicket_kind
left join gold.dim_players as po on s.player_out = po.player
left join gold.dim_players as f on s.fielders = f.player
left join gold.dim_players as rb on s.review_batter = rb.player
left join gold.dim_team tr on s.team_reviewed = tr.team
left join gold.dim_players as pom on s.player_of_match = pom.player
left join gold.dim_team mwb on s.match_won_by = mwb.team
left join gold.dim_team tw on s.toss_winner = tw.team
left join gold.dim_venue v on s.venue = v.venue
left join gold.dim_city c on s.city = c.city
left join gold.dim_team so on s.superover_winner = so.team
left join gold.dim_stage st on s.stage = st.stage
left join gold.dim_players nwb on s.new_batter = nwb.player
left join gold.dim_batting_partners bp on s.batting_partners = bp.batting_partners
left join gold.dim_players nxt on s.next_batter = nxt.player;





 

ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_batting_team_id
FOREIGN KEY (batting_team_id)
REFERENCES gold.dim_team (team_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_bowling_team_id
FOREIGN KEY (bowling_team_id)
REFERENCES gold.dim_team (team_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_team_id
FOREIGN KEY (toss_winner_id)
REFERENCES gold.dim_team (team_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_match_won
FOREIGN KEY (mwb_id)
REFERENCES gold.dim_team (team_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_super_over
FOREIGN KEY (superover_winner_id)
REFERENCES gold.dim_team (team_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_batter
FOREIGN KEY (batter_id)
REFERENCES gold.dim_players (player_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_non_striker
FOREIGN KEY (non_striker_id)
REFERENCES gold.dim_players (player_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_player_out
FOREIGN KEY (player_out_id)
REFERENCES gold.dim_players (player_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_bowler
FOREIGN KEY (bowler_id)
REFERENCES gold.dim_players (player_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_extra
FOREIGN KEY (extra_type_id)
REFERENCES gold.dim_extra_type (extra_type_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_pom
FOREIGN KEY (pom_id)
REFERENCES gold.dim_players (player_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_wicket_type
FOREIGN KEY (wicket_kind_id)
REFERENCES gold.dim_wicket_kind(wicket_type_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_fielder
FOREIGN KEY (fielder_id)
REFERENCES gold.dim_players(player_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_stage
FOREIGN KEY (stage_id)
REFERENCES gold.dim_stage(stage_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_partners
FOREIGN KEY (partnership_id)
REFERENCES gold.dim_batting_partners(partner_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_venue
FOREIGN KEY (venue_id)
REFERENCES gold.dim_venue(venue_id);
 
ALTER TABLE gold.ipl_fact_table
ADD CONSTRAINT fk_city
FOREIGN KEY (city_id)
REFERENCES gold.dim_city(city_id);
