#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import sys
import pyspark
from pyspark.sql import SparkSession, SQLContext, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

if __name__ == "__main__":
    # initialize spark session
    spark = pyspark.sql.SparkSession.builder    .appName("weather_challange")     .getOrCreate()

    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("ERROR")

    if len(sys.argv) < 4:
        print(" please provide path of all the three input files")
        spark.stop()
    else:
        weatherPath = sys.argv[1]
        countrylistPath = sys.argv[2]
        stationlistPath = sys.argv[3]
        
        
        
    schema = StructType([
        StructField("STN---", IntegerType(), False),
        StructField("WBAN", IntegerType(), False),
        StructField("YEARMODA", IntegerType(), False),
        StructField("TEMP", DoubleType(), True),
        StructField("DEWP", DoubleType(), True),
        StructField("SLP", DoubleType(), True),
        StructField("STP", DoubleType(), True),
        StructField("VISIB", DoubleType(), True),
        StructField("WDSP", DoubleType(), True),
        StructField("MXSPD", DoubleType(), True),
        StructField("GUST", DoubleType(), True),
        StructField("MAX", DoubleType(), True),
        StructField("MIN", DoubleType(), True),
        StructField("PRCP", DoubleType(), True),
        StructField("SNDP", DoubleType(), True),
        StructField("FRSHTT", StringType(), False)   
    ])
    
    weather = spark.read.csv(weatherPath,header=True,schema=schema)
    countrylist = spark.read.csv(countrylistPath,header=True)
    stationlist = spark.read.csv(stationlistPath,header=True)
    
    joinedDF = countrylist.join(stationlist, on= "COUNTRY_ABBR",how="right")
    
    
    weather_final = weather.join(joinedDF, F.col("STN---")==F.col("STN_NO"),how="inner").cache()
    
    #Q1 - Which country had the hottest average mean temperature over the year?
    TEMP_clean_df = weather_final.select("COUNTRY_FULL","TEMP").filter(F.col("TEMP")!=9999.9)
    Country_hot_mean_temp = TEMP_clean_df.groupBy(F.col("COUNTRY_FULL")).agg(F.avg(F.col("TEMP")).alias("avg_TEMP")).orderBy(F.col("avg_TEMP").desc()).collect()[0]
    print("{} has hottest mean temperation of {} among all countries".format(Country_hot_mean_temp[0],Country_hot_mean_temp[1]))
    
    #Q2 - Which country had the most consecutive days of tornadoes/funnel cloud formations?
    consecutive_tor_days = weather_final.select("COUNTRY_FULL","FRSHTT","YEARMODA").filter(F.col("FRSHTT").substr(5,1)==1)
    
    
    #Q3- Which country had the second highest average mean wind speed over the year?
    WDSP_clean_df = weather_final.select("COUNTRY_FULL","WDSP").filter(F.col("WDSP")!=999.9)
    w= Window.orderBy(F.col("avg_WDSP").desc())
    Second_highest_mean_speed_country = WDSP_clean_df.groupBy(F.col("COUNTRY_FULL"))        .agg(F.avg(F.col("WDSP")).alias("avg_WDSP"))        .orderBy(F.col("avg_WDSP").desc())        .collect()[1]
#    print("{} has second highest mean speed of {} among all countries".format(Second_highest_mean_speed_country[0],Second_highest_mean_speed_country[1]))


    
    

