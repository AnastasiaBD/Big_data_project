create schema if not exists dds_stg;
create schema if not exists dds;
create schema if not exists dm;
create schema if not exists etl;
create schema if not exists idw;


create table if not exists dds_stg.us_aqi_t (date_ date NULL,
	aqi int8 NULL,
	category text NULL,
	state_id text NULL,
	state_name text NULL,
	lat numeric NULL,
	lng numeric NULL,
	population numeric NULL,
	"source" text NULL,
	date_load timestamp NULL
);

create table if not exists dds_stg.us_county_sociohealth_data_t (
	state text NULL,
	county text NULL,
	lat numeric NULL,
	lon numeric NULL,
	total_population int8 NULL,
	num_dealths int8 NULL,
	years_of_potential_life_lost_rate numeric NULL,
	percent_smokers numeric NULL,
	percent_excessive_drinking numeric NULL,
	num_injury_deaths int8 NULL,
	overcrowding numeric NULL,
	km_to_closest_station numeric NULL,
	wind_speed numeric NULL,
	dewpoint_3d_avg numeric NULL,
	percent_below_poverty numeric NULL,
	percent_children_in_poverty numeric NULL,
	num_drug_overdose_deaths int8 NULL,
	percent_low_birthweight numeric NULL,
	percent_adults_with_obesity numeric NULL,
	percent_with_access_to_exercise_opportunities numeric NULL,
	percent_homeowners numeric NULL,
	percent_female numeric NULL,
	percent_limited_english_abilities numeric NULL,
	percent_no_highschool_diploma numeric NULL,
	percent_frequent_physical_distress numeric NULL,
	percent_frequent_mental_distress numeric NULL,
	percent_insufficient_sleep numeric NULL,
	percent_less_than_18_years_of_age numeric NULL,
	percent_65_and_over numeric NULL,
	"source" text NULL,
	date_load timestamp NULL
);

CREATE OR REPLACE FUNCTION etl.transfer_us_aqi_t__to_dds()
RETURNS void AS
$BODY$
begin
create table if not exists dds.us_aqi_t (date_ date NULL,
	aqi int8 NULL,
	category text NULL,
	state_id text NULL,
	state_name text NULL,
	lat numeric NULL,
	lng numeric NULL,
	population numeric NULL,
	"source" text NULL,
	date_load timestamp NULL
);
INSERT INTO dds.us_aqi_t (date_, aqi, category, state_id, state_name, lat, lng, population, "source", date_load)
SELECT date_, aqi, category, state_id, state_name, lat, lng, population, "source", date_load
FROM dds_stg.us_aqi_t;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.transfer_us_county_sociohealth_data_t__to_dds()
RETURNS void AS
$BODY$
begin
create table if not exists dds.us_county_sociohealth_data_t (
	state text NULL,
	county text NULL,
	lat numeric NULL,
	lon numeric NULL,
	total_population int8 NULL,
	num_dealths int8 NULL,
	years_of_potential_life_lost_rate numeric NULL,
	percent_smokers numeric NULL,
	percent_excessive_drinking numeric NULL,
	num_injury_deaths int8 NULL,
	overcrowding numeric NULL,
	km_to_closest_station numeric NULL,
	wind_speed numeric NULL,
	dewpoint_3d_avg numeric NULL,
	percent_below_poverty numeric NULL,
	percent_children_in_poverty numeric NULL,
	num_drug_overdose_deaths int8 NULL,
	percent_low_birthweight numeric NULL,
	percent_adults_with_obesity numeric NULL,
	percent_with_access_to_exercise_opportunities numeric NULL,
	percent_homeowners numeric NULL,
	percent_female numeric NULL,
	percent_limited_english_abilities numeric NULL,
	percent_no_highschool_diploma numeric NULL,
	percent_frequent_physical_distress numeric NULL,
	percent_frequent_mental_distress numeric NULL,
	percent_insufficient_sleep numeric NULL,
	percent_less_than_18_years_of_age numeric NULL,
	percent_65_and_over numeric NULL,
	"source" text NULL,
	date_load timestamp NULL
);
INSERT INTO dds.us_county_sociohealth_data_t (state, county, lat, lon, total_population, num_dealths, years_of_potential_life_lost_rate, percent_smokers, percent_excessive_drinking, num_injury_deaths, overcrowding, km_to_closest_station, wind_speed, dewpoint_3d_avg, percent_below_poverty, percent_children_in_poverty, num_drug_overdose_deaths, percent_low_birthweight, percent_adults_with_obesity, percent_with_access_to_exercise_opportunities, percent_homeowners, percent_female, percent_limited_english_abilities, percent_no_highschool_diploma, percent_frequent_physical_distress, percent_frequent_mental_distress, percent_insufficient_sleep, percent_less_than_18_years_of_age, percent_65_and_over, "source", date_load)
SELECT state, county, lat, lon, total_population, num_dealths, years_of_potential_life_lost_rate, percent_smokers, percent_excessive_drinking, num_injury_deaths, overcrowding, km_to_closest_station, wind_speed, dewpoint_3d_avg, percent_below_poverty, percent_children_in_poverty, num_drug_overdose_deaths, percent_low_birthweight, percent_adults_with_obesity, percent_with_access_to_exercise_opportunities, percent_homeowners, percent_female, percent_limited_english_abilities, percent_no_highschool_diploma, percent_frequent_physical_distress, percent_frequent_mental_distress, percent_insufficient_sleep, percent_less_than_18_years_of_age, percent_65_and_over, "source", date_load
FROM dds_stg.us_county_sociohealth_data_t;
END;
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION etl.transfer_to_addictions_dm()
RETURNS void AS
$BODY$
begin
create table dm.people_with_addictions_t (state text, county text, percent_smokers numeric, percent_excessive_drinking numeric, percent_frequent_physical_distress numeric, percent_frequent_mental_distress numeric);
insert into dm.people_with_addictions_t (state, county, percent_smokers, percent_excessive_drinking, percent_frequent_physical_distress, percent_frequent_mental_distress)
select state, county, percent_smokers, percent_excessive_drinking, percent_frequent_physical_distress, percent_frequent_mental_distress
from dds.us_county_sociohealth_data_t;
delete from dm.people_with_addictions_t where ctid not in 
(select max(ctid) from dm.people_with_addictions_t group by dm.people_with_addictions_t.state);
END;
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION etl.transfer_to_birthweight_dm()
RETURNS void AS
$BODY$
begin
create table dm.birthweight_t (state text,lat numeric, lon numeric, percent_low_birthweight numeric, percent_children_in_poverty numeric);
insert into dm.birthweight_t
select state, lat, lon, percent_low_birthweight, percent_children_in_poverty
from dds.us_county_sociohealth_data_t
order by state asc;
delete from dm.birthweight_t where ctid not in 
(select max(ctid) from dm.birthweight_t group by dm.birthweight_t.state);
END;
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION etl.transfer_air_pollution_dm()
RETURNS void AS
$BODY$
begin
create table dm.air_pollution_t (state text,lat numeric, lon numeric, avg_aqi int8, percent_low_birthweight numeric, percent_adults_with_obesity numeric); 
create view idw.tmp_air_v as
select state_name, avg(aqi) as avg_aqi
from dds.us_aqi_t 
group by state_name;
create view idw.tmp_state_v as
select state, lat, lon, percent_low_birthweight, percent_adults_with_obesity 
from dds.us_county_sociohealth_data_t;
insert into dm.air_pollution_t
select idw.tmp_state_v.state, idw.tmp_state_v.lat, idw.tmp_state_v.lon, idw.tmp_air_v.avg_aqi, idw.tmp_state_v.percent_low_birthweight, idw.tmp_state_v.percent_adults_with_obesity
from idw.tmp_air_v inner join idw.tmp_state_v on
idw.tmp_state_v.state = idw.tmp_air_v.state_name
order by state asc;
delete from dm.air_pollution_t where ctid not in 
(select max(ctid) from dm.air_pollution_t group by dm.air_pollution_t.state);
END;
$BODY$
LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION etl.corr_of_first_hypothesis_t()
RETURNS void AS
$BODY$
begin
create table dm.corr_of_first_hypothesis_t (smoking_from_physical_distress numeric, smoking_from_mental_distress numeric, drinking_from_physical_distress numeric, drinking_from_mental_distress numeric);
insert into dm.corr_of_first_hypothesis_t (smoking_from_physical_distress, smoking_from_mental_distress, drinking_from_physical_distress, drinking_from_mental_distress)
select corr (percent_smokers, percent_frequent_physical_distress) as smoking_from_physical_distress, 
corr (percent_smokers,percent_frequent_mental_distress) as smoking_from_mental_distress,
corr (percent_excessive_drinking, percent_frequent_physical_distress) as drinking_from_physical_distress, 
corr (percent_excessive_drinking, percent_frequent_mental_distress) as drinking_from_mental_distress
from dm.people_with_addictions_t;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.corr_of_second_hypothesis_t()
RETURNS void AS
$BODY$
begin
create table dm.corr_of_second_hypothesis_t (obesity_from_aqi numeric, birthweight_from_aqi numeric);
insert into dm.corr_of_second_hypothesis_t (obesity_from_aqi, birthweight_from_aqi)
select corr(avg_aqi, percent_adults_with_obesity) as obesity_from_aqi,
corr (avg_aqi, percent_low_birthweight) as birthweight_from_aqi
from dm.air_pollution_t;
END;
$BODY$
LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION etl.corr_of_third_hypothesis_t()
RETURNS void AS
$BODY$
begin
create table dm.corr_of_third_hypothesis_t (birthweight_from_poverty numeric);
insert into dm.corr_of_third_hypothesis_t (birthweight_from_poverty)
select corr(percent_low_birthweight, percent_children_in_poverty) as birthweight_from_poverty
from dm.birthweight_t;
END;
$BODY$
LANGUAGE plpgsql;
