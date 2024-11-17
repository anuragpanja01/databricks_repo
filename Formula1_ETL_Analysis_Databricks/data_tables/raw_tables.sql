-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS formula1_raw

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.circuits;
CREATE TABLE IF NOT EXISTS formula1_raw.circuits(
  circuitId INT,
  circuitRef STRING,
  name  STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING                                 
)
USING CSV
OPTIONS (path "/mnt/fraw/circuits.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.races;
CREATE TABLE IF NOT EXISTS formula1_raw.races(
          raceId INT,
          year INT,
          round INT,
          circuitId INT,
          name STRING,
          date DATE,
          time STRING,
          url STRING                                                                  
)
USING CSV
OPTIONS (path "/mnt/fraw/races.csv", header true)

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.constructors;
CREATE TABLE IF NOT EXISTS formula1_raw.constructors(
          constructorId INT,
          constructorRef STRING,
          name STRING,
          nationality STRING,
          url STRING                                                               
)
USING JSON
OPTIONS (path "/mnt/fraw/constructors.json")

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.drivers;
CREATE TABLE IF NOT EXISTS formula1_raw.drivers(
        driverId INT, 
        driverRef STRING, 
        number INT, 
        code STRING, 
        name STRUCT<forename: STRING, surname: STRING>, 
        dob DATE, 
        nationality STRING, 
        url STRING                                                          
)
USING JSON
OPTIONS (path "/mnt/fraw/drivers.json")

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.results;
CREATE TABLE IF NOT EXISTS formula1_raw.results(
        resultId INT, 
        raceId INT, 
        driverId INT, 
        constructorId INT, 
        number INT, 
        grid INT, 
        position INT, 
        positionText STRING, 
        positionOrder INT, 
        points FLOAT, 
        laps INT, 
        time STRING, 
        milliseconds INT, 
        fastestLap INT, 
        rank INT, 
        fastestLapTime STRING, 
        fastestLapSpeed FLOAT, 
        statusId STRING                                                       
)
USING JSON
OPTIONS (path "/mnt/fraw/results.json")

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS formula1_raw.pit_stops(
        raceId INT, 
        driverId INT, 
        stop STRING, 
        lap INT, 
        time STRING, 
        duration STRING, 
        milliseconds INT                                                    
)
USING JSON
OPTIONS (path "/mnt/fraw/pit_stops.json", multiLine true)

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.lap_times;
CREATE TABLE IF NOT EXISTS formula1_raw.lap_times(
        raceId INT,
        driverId INT, 
        lap INT, 
        position INT, 
        time STRING, 
        milliseconds INT                               
)
USING CSV
OPTIONS (path "/mnt/fraw/lap_times/", header true)


-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.laptimes;

-- COMMAND ----------

DROP TABLE IF EXISTS formula1_raw.qualifying;
CREATE TABLE IF NOT EXISTS formula1_raw.qualifying(
        qualifyId INT,
        raceId INT, 
        driverId INT, 
        constructorId INT, 
        number INT, 
        position INT, 
        q1 STRING, 
        q2 STRING, 
        q3 STRING                                                       
)
USING JSON
OPTIONS (path "/mnt/fraw/qualifying/" , multiLine true)

-- COMMAND ----------

SELECT * FROM formula1_raw.qualifying;

-- COMMAND ----------

DESC EXTENDED formula1_raw.qualifying;
