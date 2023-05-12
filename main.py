
import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect(host="localhost", port=5000, database="RadaevaBD", user="radaeva", password="radaevapwd")
cursor = conn.cursor()

if __name__ == "__main__":
    for data in pd.read_csv("/home/radaeva/hive/US_counties_COVID19_health_weather_data.csv", chunksize=100000):
        conn = psycopg2.connect(host="localhost", port=5000, database="RadaevaBD", user="radaeva", password="radaevapwd")
        cursor = conn.cursor()
        print(data)

        data = data.replace('Nan', np.nan).fillna(value=data.mean())
        for i in data.values:
            if any(pd.isnull(i)):
                continue
            #cursor.execute("INSERT INTO stg.test (state, lat, lon, total_population, num_dealths, years_of_potential_life_lost_rate, percent_smokers, percent_excessive_drinking, num_injury_deaths, overcrowding, km_to_closest_station, wind_speed, dewpoint_3d_avg, percent_below_poverty, percent_children_in_poverty, num_drug_overdose_deaths, percent_low_birthweight, source, date_load) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'USCounty', now());", [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14], i[15], i[16]])
            cursor.execute("INSERT INTO dds_stg.us_county_sociohealth_data_t (state, county, lat, lon, total_population, num_dealths, years_of_potential_life_lost_rate, percent_smokers, percent_excessive_drinking, num_injury_deaths, overcrowding, km_to_closest_station, wind_speed, dewpoint_3d_avg, percent_below_poverty, percent_children_in_poverty, num_drug_overdose_deaths, percent_low_birthweight, percent_adults_with_obesity, percent_with_access_to_exercise_opportunities, percent_homeowners, percent_female, percent_limited_english_abilities, percent_no_highschool_diploma, percent_frequent_physical_distress, percent_frequent_mental_distress, percent_insufficient_sleep, percent_less_than_18_years_of_age, percent_65_and_over, source, date_load) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'USCounty', now());",[i[2], i[1], i[8], i[9], i[10], i[13], i[14], i[19], i[22], i[60], i[66], i[186], i[198], i[221], i[152], i[49], i[87], i[18], i[20], i[23], i[112], i[132], i[160], i[154], i[78], i[79], i[91], i[116], i[117]])
        conn.commit()
        conn.close()
        print('Good')