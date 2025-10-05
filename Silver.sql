create schema if not exists silver;

drop table if exists silver.updated_ipl_data;

create table silver.updated_ipl_data(
match_id INT,
date VARCHAR,
match_type VARCHAR,
event_name	VARCHAR,
innings	INT,
batting_team VARCHAR,
bowling_team VARCHAR,
over INT,
ball INT,
ball_no INT,
batter	VARCHAR,
bat_pos INT,
runs_batter INT,
balls_faced	INT,
bowler VARCHAR,
valid_ball INT,
runs_extras	INT,
runs_total INT,
runs_bowler INT,
runs_not_boundary VARCHAR,
extra_type VARCHAR,
non_striker VARCHAR,
non_striker_pos INT,
wicket_kind VARCHAR,
player_out VARCHAR,
fielders VARCHAR,
runs_target	INT,
review_batter VARCHAR,
team_reviewed VARCHAR,
review_decision	VARCHAR,
umpire VARCHAR,
umpires_call VARCHAR,
player_of_match VARCHAR,
match_won_by VARCHAR,
win_outcome	VARCHAR,
toss_winner	VARCHAR,
toss_decision VARCHAR,
venue VARCHAR,
city VARCHAR,
day	INT,
month INT,
year INT,	
season VARCHAR,
gender 	VARCHAR,
team_type	 VARCHAR,
superover_winner VARCHAR,
result_type	VARCHAR,
method	VARCHAR,
balls_per_over INT,
overs INT,	
event_match_no	INT,
stage VARCHAR,	
match_number VARCHAR,	 
team_runs INT,
team_balls	INT,
team_wicket	INT,
new_batter	VARCHAR,
batter_runs	INT,
batter_balls INT	,
bowler_wicket INT	,
batting_partners VARCHAR,
next_batter	varchar,
striker_out VARCHAR
);

ALTER TABLE silver.updated_ipl_data alter column ball_no type decimal;
ALTER TABLE silver.updated_ipl_data alter column event_match_no type VARCHAR;

insert into silver.updated_ipl_data (select * from bronze.raw_ipl);

update silver.updated_ipl_data set batting_team = 'Punjab Kings' where batting_team like '%Punj%';
update silver.updated_ipl_data set batting_team = 'Rising Pune Supergiants' where batting_team like '%Pune%';
update silver.updated_ipl_data set batting_team = 'Royal Challengers Bengaluru' where batting_team like '%Challengers%';

update silver.updated_ipl_data set bowling_team = 'Punjab Kings' where bowling_team like '%Punj%';
update silver.updated_ipl_data set bowling_team = 'Rising Pune Supergiants' where bowling_team like '%Pune%';
update silver.updated_ipl_data set bowling_team = 'Royal Challengers Bengaluru' where bowling_team like '%Challengers%';

update silver.updated_ipl_data set match_won_by = 'Punjab Kings' where match_won_by like '%Punj%';
update silver.updated_ipl_data set match_won_by = 'Rising Pune Supergiants' where match_won_by like '%Pune%';
update silver.updated_ipl_data set match_won_by = 'Royal Challengers Bengaluru' where match_won_by like '%Challengers%';

update silver.updated_ipl_data set venue = 'Arun Jaitley Stadium' where venue like '%Jaitley%';
update silver.updated_ipl_data set venue = 'Barsapara Cricket Stadium' where venue like '%Barsapara%';
update silver.updated_ipl_data set venue = 'Bharat Ratna Shri Atal Bihari Vajpayee Ekana Cricket Stadium' 
where venue like '%Vajpayee %';
update silver.updated_ipl_data set venue = 'Brabourne Stadium' where venue like '%Brabourne%';
update silver.updated_ipl_data set venue = 'Dr DY Patil Sports Academy' where venue like '%Patil%';
update silver.updated_ipl_data set venue = 'Dr. Y.S. Rajasekhara Reddy ACA-VDCA Cricket Stadium' 
where venue like '%Rajasekhara%';
update silver.updated_ipl_data set venue = 'Eden Gardens' where venue like '%Eden%';
update silver.updated_ipl_data set venue = 'Himachal Pradesh Cricket Association Stadium' where venue like '%Himachal%';
update silver.updated_ipl_data set venue = 'M Chinnaswamy Stadium' where venue like '%Chinnaswamy%';
update silver.updated_ipl_data set venue = 'MA Chidambaram Stadium' where venue like '%Chidambaram%';
update silver.updated_ipl_data set venue = 'Maharaja Yadavindra Singh International Cricket Stadium'
where venue like '%Yadavindra%';
update silver.updated_ipl_data set venue = 'Maharashtra Cricket Association Stadium' where venue like '%Maharashtra%';
update silver.updated_ipl_data set venue = 'Punjab Cricket Association Stadium' where venue like '%Punjab%';
update silver.updated_ipl_data set venue = 'Rajiv Gandhi International Stadium' where venue like '%Rajiv%';
update silver.updated_ipl_data set venue = 'Sardar Patel Stadium' where venue like '%Sardar%';
update silver.updated_ipl_data set venue = 'Sawai Mansingh Stadium' where venue like '%Mansingh%';
update silver.updated_ipl_data set venue = 'Vidarbha Cricket Association Stadium' where venue like '%Vidarbha%';
update silver.updated_ipl_data set venue = 'Wankhede Stadium' where venue like '%Wankhede%';
update silver.updated_ipl_data set venue = 'Arun Jaitley Stadium' where venue = 'Feroz Shah Kotla';



update silver.updated_ipl_data set toss_winner = 'Punjab Kings' where toss_winner like '%Punj%';
update silver.updated_ipl_data set toss_winner = 'Rising Pune Supergiants' where toss_winner like '%Pune%';
update silver.updated_ipl_data set toss_winner = 'Royal Challengers Bengaluru' where toss_winner like '%Challengers%';

update silver.updated_ipl_data set team_reviewed = 'Punjab Kings' where team_reviewed like '%Punj%';
update silver.updated_ipl_data set team_reviewed = 'Rising Pune Supergiants' where team_reviewed like '%Pune%';
update silver.updated_ipl_data set team_reviewed = 'Royal Challengers Bengaluru' where team_reviewed like '%Challengers%';

update silver.updated_ipl_data set superover_winner = 'Punjab Kings' where superover_winner like '%Punj%';
update silver.updated_ipl_data set superover_winner = 'Rising Pune Supergiants' where superover_winner like '%Pune%';
update silver.updated_ipl_data set superover_winner = 'Royal Challengers Bengaluru' where superover_winner like '%Challengers%';

update silver.updated_ipl_data set city = 'Bengaluru' where city like '%Bang%';
update silver.updated_ipl_data set city = 'Dubai Sports City' where venue like '%Dubai%';
update silver.updated_ipl_data set city = 'Sharjah' where venue like '%Sharjah%';
update silver.updated_ipl_data set stage = 'league_matches' where stage like 'Unknown';
update silver.updated_ipl_data set fielders = split_part(fielders, ',', 1);
 
ALTER TABLE silver.updated_ipl_data
ALTER COLUMN date TYPE DATE USING TO_DATE(date, 'MM/DD/YYYY');

alter table silver.updated_ipl_data drop column umpire;
alter table silver.updated_ipl_data drop column event_match_no;

