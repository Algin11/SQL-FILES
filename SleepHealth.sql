-- Duplication Of Table for Data Manipulation
CREATE DATABASE IF NOT EXISTS Sleephealth;

USE sleep_health;

CREATE TABLE Sleephealth
(	Person_ID INT 
  ,	Gender VARCHAR(225)
  ,	Age INT 
  ,	Occupation VARCHAR(225)
  ,	Sleep_Duration float
  ,	Quality_of_Sleep INT 
  ,	Physical_Activity_Level INT 
  ,	Stress_Level INT 
  ,	BMI_Category VARCHAR(225) 
  ,	Blood_Pressure VARCHAR(225)
  ,	Heart_Rate INT 
  ,	Daily_Steps INT
  ,	Sleep_Disorder VARCHAR(225) )
  
  
INSERT INTO sleephealth
SELECT *
FROM sleep_health.sleep_health_and_lifestyle_dataset 

-- Data Cleaning
-- Cheking for Duplicate
WITH DUP AS (
SELECT	*
	,	ROW_NUMBER() OVER (PARTITION BY  Person_ID
					   ,	 Gender
                                           , 	 Age
                                           ,	 Occupation
                                           ,	 Sleep_Duration
                                           ,	 Quality_of_Sleep
                                           ,	 Physical_Activity_Level
                                           ,	 Stress_Level
                                           ,	 BMI_Category
                                           ,	 Blood_Pressure
                                           ,	 Heart_Rate
                                           ,	 Daily_Steps
                                           ,	 Sleep_Disorder ) AS Row_Numb
FROM sleephealth )

SELECT *
FROM DUP 
WHERE Row_Numb > 1

-- Checking For Nulls 
SELECT *
FROM sleep_health.sleephealth
WHERE	Person_ID IS NULL
    OR	Gender IS NULL
    OR	Age IS NULL
    OR	Occupation IS NULL
    OR	Sleep_Duration IS NULL
    OR	Quality_of_Sleep IS NULL
    OR	Physical_Activity_Level IS NULL
    OR	Stress_Level IS NULL
    OR	BMI_Category IS NULL
    OR	Blood_Pressure IS NULL
    OR	Heart_Rate IS NULL
    OR	Daily_Steps IS NULL
    OR	Sleep_Disorder IS NULL 


-- Data Manipulation

-- Updating the BMI_Category
UPDATE sleep_health.sleep_health_and_lifestyle_dataset 
SET BMI_Category = 'Normal'
WHERE BMI_Category = 'Normal Weight'

-- Number of People Per Occupation
SELECT	Occupation
,	COUNT(occupation) AS Number_of_people
FROM sleep_health.sleephealth
GROUP BY Occupation
ORDER BY number_of_people DESC

-- BMI Category Group By Number Of Peolple
SELECT	BMI_category
,	COUNT(BMI_category) AS Number_of_people
FROM sleep_health.sleephealth
GROUP BY   BMI_category

-- Occupation Group By Gender And Age
SELECT	Occupation
,	Gender
,	COUNT(GENDER) AS Count_of_gender
,	ROUND(Avg(Age) , 0) AS Avg_age
,	MAX(Age) AS Max_age
,	MIN(Age) AS Min_age
FROM sleep_health.sleephealth
GROUP BY Occupation, Gender
ORDER BY Occupation, Gender DESC

-- Table For Average Sleep, Physical Activity, Stress Level, Heart Rate and Daily Steps
SELECT	Occupation
,	Gender
,	ROUND(AVG(AGE),2) AS Avg_age
,	ROUND(AVG(Sleep_Duration),2) AS Avg_sleep_duration
,	ROUND(AVG(Quality_of_sleep),2) AS Avg_qty_of_sleep
,	ROUND(AVG(Physical_Activity_Level),2) AS Avg_physical_activity_level
,	ROUND(AVG(Stress_Level),2) AS Avg_stress_level
,	ROUND(AVG(Heart_Rate),2) AS Avg_hearth_rate
,	ROUND(AVG(Daily_Steps),2) AS Avg_daily_steps
FROM sleep_health.sleephealth
GROUP BY Occupation, Gender
ORDER BY Occupation, Gender DESC

-- Stress Level Of A Person With Sleeping Disorders
SELECT Occupation
,	Sleep_Disorder
,	COUNT(Sleep_Disorder) AS Count_Disorder
,	ROUND(AVG(Stress_Level),2) AS Stess_level
FROM sleep_health.sleephealth
GROUP BY Occupation, Sleep_Disorder
ORDER BY Occupation, Sleep_Disorder

-- Physical Activity Of A Person With Sleeping Disorders
SELECT Occupation
,	Sleep_Disorder
,	COUNT(Sleep_Disorder) AS Count_Disorder
,	ROUND(AVG(Physical_Activity_Level),2) AS Physical_Activity_Level
FROM sleep_health.sleephealth
GROUP BY Occupation, Sleep_Disorder
ORDER BY Occupation, Sleep_Disorder

-- Quality Of Sleep Of A Person With Sleeping Disorders
SELECT Occupation
,	Sleep_Disorder
,	COUNT(Sleep_Disorder) AS Count_Disorder
,	ROUND(AVG(Quality_of_Sleep),2) AS Quality_of_sleep
FROM sleep_health.sleephealth
GROUP BY Occupation, Sleep_Disorder
ORDER BY  Occupation, Sleep_Disorder

-- Sleeping Disorder Group By Gender
SELECT	Gender
,	Sleep_Disorder
,	COUNT(Sleep_Disorder) AS Count_Sleep_Disorder
,	ROUND((COUNT(Sleep_Disorder) / SUM(COUNT(Sleep_Disorder))  OVER (PARTITION  BY Gender) )*100,2) AS Percentage
FROM sleep_health.sleephealth
GROUP BY GENDER, Sleep_Disorder
ORDER BY Gender DESC, Count_Sleep_Disorder DESC

-- Sleep Disorder Group By Occupation
SELECT	Occupation
,	Sleep_Disorder
,	COUNT(Sleep_Disorder) AS Count_Sleep_Disorder
,	ROUND((COUNT(Sleep_Disorder) / SUM(COUNT(Sleep_Disorder)) OVER (PARTITION BY OCCUPATION))*100,2) AS Percentage
FROM sleep_health.sleephealth
GROUP BY Occupation, Sleep_Disorder
ORDER BY Occupation, Count_Sleep_Disorder DESC

-- Average Age Of Sleeping Disorder
SELECT Sleep_Disorder
,	ROUND(AVG(AGE),0) AS Age
FROM sleep_health.sleephealth
GROUP BY Sleep_Disorder

-- Count Of Genders
SELECTGENDER
,	COUNT(Gender) AS Count_Gender
,	(COUNT(Gender) / SUM(COUNT(Gender)) OVER ()) * 100 AS Percentage
FROM sleep_health.sleephealth
GROUP BY GENDER

-- Count of Genders Group By Occupation
SELECT	Occupation
,	Gender
,	COUNT(Gender) AS Count_Gender
FROM sleep_health.sleephealth
GROUP BY Occupation , Gender
ORDER BY Occupation, Gender DESC

-- BMI Category Group by Gender 
SELECT	DISTINCT(Gender)
,	BMI_Category
,	COUNT(BMI_Category) AS Count_BMI_Category
,	ROUND(COUNT(BMI_Category)/SUM(COUNT(BMI_Category))  OVER (PARTITION BY GENDER) * 100,2) AS Percentage
FROM sleep_health.sleephealth     
GROUP BY Gender, BMI_Category
ORDER BY Gender DESC










