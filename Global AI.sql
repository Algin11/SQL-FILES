-- Importing  The Dataset For Data Manipualtions
CREATE DATABASE IF NOT EXISTS global_ai;

USE global_ai;
CREATE TABLE global_ai
( 	Country VARCHAR(225)
 ,	Year INT
 ,	Industry VARCHAR(225)
 ,	`AI_Adoption_Rate_(%)` FLOAT
 ,	`AI-Generated_Content_Volume_(TBs_per_year)` FLOAT
 ,	`Job_Loss_Due_to_AI_(%)` FLOAT
 ,	`Revenue_Increase_Due_to_AI_(%)` FLOAT
 ,	`Human-AI_Collaboration_Rate_(%)` FLOAT
 ,	Top_AI_Tools_Used VARCHAR(225)
 ,	Regulation_Status VARCHAR(225)
 ,	`Consumer_Trust_in_AI_(%)` FLOAT
 ,	`Market_Share_of_AI_Companies_(%)` FLOAT )
 
SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:Data Analyst/Data analyst/PROJECTS/Global AI/Global_AI_Content_Impact_Dataset.csv' INTO TABLE global_ai
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


-- Checking for Duplicate
WITH Dup AS(
SELECT 	*
	,	Row_Number() OVER (PARTITION BY	 	Country
										,	Year
                                        ,	Industry
                                        ,	`AI_Adoption_Rate_(%)`
                                        ,	`AI-Generated_Content_Volume_(TBs_per_year)`
                                        ,	`Job_Loss_Due_to_AI_(%)`
                                        ,	`Revenue_Increase_Due_to_AI_(%)`
                                        ,	`Human-AI_Collaboration_Rate_(%)`
                                        ,	Top_AI_Tools_Used
                                        ,	Regulation_Status
                                        ,	`Consumer_Trust_in_AI_(%)`
                                        ,	`Market_Share_of_AI_Companies_(%)` ) AS Row_Numb
                                        
FROM global_ai .global_ai )

SELECT *
FROM Dup
WHERE Row_Numb > 1

-- Checking for NULL
SELECT *
FROM global_ai.global_ai
WHERE  Country IS NULL
	OR	Year  IS NULL
    OR Industry IS NULL
    OR `AI_Adoption_Rate_(%)` IS NULL
    OR `AI-Generated_Content_Volume_(TBs_per_year)` IS NULL
    OR `Job_Loss_Due_to_AI_(%)` IS NULL
    OR `Revenue_Increase_Due_to_AI_(%)` IS NULL
    OR `Human-AI_Collaboration_Rate_(%)` IS NULL
    OR Top_AI_Tools_Used IS NULL
    OR Regulation_Status IS NULL
    OR `Consumer_Trust_in_AI_(%)`IS NULL
    OR `Market_Share_of_AI_Companies_(%)` IS NULL


-- Data Manipulation
-- Highest AI Generated Volume every Year
SELECT	Country
	,	Year
    ,	`AI-Generated_Content_Volume_(TBs_per_year)`
    ,	Top_AI_Tools_Used
FROM  global_ai.global_ai
WHERE `AI-Generated_Content_Volume_(TBs_per_year)` in ( SELECT MAX(`AI-Generated_Content_Volume_(TBs_per_year)`)
														FROM  global_ai.global_ai
                                                        GROUP BY YEAR)
ORDER BY YEAR DESC

-- Average AI Generated Volume Per Year
SELECT	DISTINCT(Country)
    ,	ROUND(AVG(`AI-Generated_Content_Volume_(TBs_per_year)`) OVER (PARTITION BY Country),2) AS AI_Generated_Content_Volume
FROM  global_ai.global_ai
ORDER BY AI_Generated_Content_Volume DESC

-- The Most Used AI in every Year
 WITH CTE AS (
				SELECT  YEAR 
					,	Top_AI_Tools_Used
					,	COUNT(Top_AI_Tools_Used) * 1.0  AS Count
				FROM global_ai.global_ai
				GROUP BY YEAR, Top_AI_Tools_Used
								)
	,	RankedCTE AS (
						SELECT 	*
							,	RANK() OVER (PARTITION BY YEAR ORDER BY Count DESC) AS ranking
						FROM CTE )
								
SELECT 	YEAR
	,	Top_AI_Tools_Used, Count
FROM RankedCTE 
WHERE ranking = 1
ORDER BY YEAR DESC
        
-- The most Used tool All The Time(2020 to 2025)
SELECT  	distinct(Top_AI_Tools_Used)
	,		COUNT(Top_AI_Tools_Used) AS Count
FROM global_ai.global_ai
GROUP BY Top_AI_Tools_Used
ORDER BY Count DESC

-- Revenue Increased per year
SELECT 	Country
	,	Year
    ,	`Revenue_Increase_Due_to_AI_(%)`
FROM global_ai.global_ai
ORDER BY 	Year DESC
		,	`Revenue_Increase_Due_to_AI_(%)` DESC
        
-- Market ShareHolder of AI tools 
  SELECT  	Year
		,	Top_AI_Tools_Used
		,	`Market_Share_of_AI_Companies_(%)`
        ,	Country
  FROM global_ai.global_ai
  ORDER BY 	YEAR DESC
		,	`Market_Share_of_AI_Companies_(%)` DESC

-- Highest Market ShareHolder per year
 SELECT  	Year
		,	Top_AI_Tools_Used
		,	`Market_Share_of_AI_Companies_(%)`
        ,	Country
  FROM global_ai.global_ai
  WHERE `Market_Share_of_AI_Companies_(%)` IN (	SELECT MAX(`Market_Share_of_AI_Companies_(%)`)
												FROM global_ai.global_ai
                                                GROUP BY YEAR)
  ORDER BY 	YEAR DESC
  
  -- Table For Job loss due to AI
SELECT  	Year
		,	Country
        ,	Industry
        ,	Top_AI_Tools_Used
		,	`Job_Loss_Due_to_AI_(%)`
FROM global_ai.global_ai
ORDER BY YEAR DESC, `Job_Loss_Due_to_AI_(%)` DESC

--  Average Job loss Per Year due to AI
SELECT  	Year
		,	ROUND(AVG(`Job_Loss_Due_to_AI_(%)`), 2) AS Percent_Job_Loss
FROM global_ai.global_ai
GROUP BY Year
ORDER BY YEAR DESC 

-- Industry With the Highest Jobloss from 2020 to 2025 due to AI
SELECT	Industry
	,	ROUND(AVG(`Job_Loss_Due_to_AI_(%)`),2) AS Job_Loss
FROM global_ai.global_ai
GROUP BY Industry
ORDER BY Job_Loss DESC

--  AI Adoption Rate per year
SELECT	Year
	,	Country
    ,	`AI_Adoption_Rate_(%)`
FROM global_ai.global_ai
ORDER BY YEAR DESC , `AI_Adoption_Rate_(%)` DESC

-- Countries AI adoption rate from 2020 to 2025
SELECT	Country
	,	ROUND(AVG(`AI_Adoption_Rate_(%)`),2) AS Adoption_Rate
FROM global_ai.global_ai
GROUP BY Country
ORDER BY Adoption_Rate DESC

-- Human AI Collaboration based on Industry from 2020 to 2025
SELECT 	Industry
	,	ROUND(AVG(`Human-AI_Collaboration_Rate_(%)`),2) AS Collaboration_Rate
FROM global_ai.global_ai
GROUP BY Industry
ORDER BY Collaboration_Rate DESC

-- Human AI Collaboration based on Country from 2020 to 2025
SELECT 	Country
	,	ROUND(AVG(`Human-AI_Collaboration_Rate_(%)`),2) AS Collaboration_Rate
FROM global_ai.global_ai
GROUP BY Country
ORDER BY Collaboration_Rate DESC

-- Average Human AI Collaboration Per Year
SELECT 	Year
	,	ROUND(AVG(`Human-AI_Collaboration_Rate_(%)`),2) AS Collaboration_Rate
FROM global_ai.global_ai
GROUP BY Year
ORDER BY Collaboration_Rate DESC

-- Industries with Consumer Trust in AI from 2020 to 2025
SELECT 	Industry
	,	ROUND(AVG(`Consumer_Trust_in_AI_(%)`),2) AS Consumer_Trust_Percentage
FROM global_ai.global_ai
GROUP BY Industry
ORDER BY Consumer_Trust_Percentage DESC

-- Consumer Trust in AI from 2020 to 2025
SELECT 	Year
	,	ROUND(AVG(`Consumer_Trust_in_AI_(%)`),2) AS Consumer_Trust_Percentage
FROM global_ai.global_ai
GROUP BY Year
ORDER BY Year DESC

-- Countires Consumer Trust
SELECT 	Country
	,	ROUND(AVG(`Consumer_Trust_in_AI_(%)`),2) AS Consumer_Trust_Percentage
FROM global_ai.global_ai
GROUP BY Country
ORDER BY Consumer_Trust_Percentage DESC


 