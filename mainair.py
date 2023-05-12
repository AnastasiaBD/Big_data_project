import psycopg2
import pandas as pd
import numpy as np

conn = psycopg2.connect(host="localhost", port=5000, database="RadaevaBD", user="radaeva", password="radaevapwd")
cursor = conn.cursor()

if __name__ == "__main__":
    for data in pd.read_csv("/home/radaeva/hive/US_AQI.csv", chunksize=100000):
        conn = psycopg2.connect(host="localhost", port=5000, database="RadaevaBD", user="radaeva", password="radaevapwd")
        cursor = conn.cursor()
        print(data)

        data = data.replace('Nan', np.nan).fillna(value=data.mean())
        for i in data.values:
            if any(pd.isnull(i)):
                continue
            #cursor.execute("INSERT INTO stg.test (state, lat, lon, total_population, num_dealths, years_of_potential_life_lost_rate, percent_smokers, percent_excessive_drinking, num_injury_deaths, overcrowding, km_to_closest_station, wind_speed, dewpoint_3d_avg, percent_below_poverty, percent_children_in_poverty, num_drug_overdose_deaths, percent_low_birthweight, source, date_load) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'USCounty', now());", [i[0], i[1], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9], i[10], i[11], i[12], i[13], i[14], i[15], i[16]])
            cursor.execute("INSERT INTO dds_stg.us_aqi_t2 (date_, aqi, category, state_id, state_name, lat, lng, population, source, date_load) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'USCounty', now());",[i[2], i[3], i[4], i[8], i[9], i[10], i[11], i[12]])
        conn.commit()
        conn.close()
        print('Good')
