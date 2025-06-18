-- Creating Table For Data Manipulation
CREATE DATABASE IF NOT EXISTS coverage_by_country;

USE hiv_cases;

CREATE TABLE coverage_by_country
( 	Country VARCHAR(225)
,	Reported_number_of_people_receiving_ART INT 
,	Estimated_number_of_people_living_with_HIV_median BIGINT 
,	Estimated_number_of_people_living_with_HIV_min BIGINT 
,	Estimated_number_of_people_living_with_HIV_max BIGINT 
,	`Estimated_ART_coverage_among_people_living_with_HIV_(%)_median` BIGINT
,	`Estimated_ART_coverage_among_people_living_with_HIV_(%)_min` BIGINT
,	`Estimated_ART_coverage_among_people_living_with_HIV_(%)_max` BIGINT
,	WHO_Region VARCHAR(255) )

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/HIV CASES/Raw files/art_coverage_by_country_clean.csv' INTO TABLE coverage_by_country
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


CREATE DATABASE IF NOT EXISTS pediatric_coverage_by_country;

USE hiv_cases;

CREATE TABLE pediatric_coverage_by_country
( 	Country VARCHAR(225)
,	Reported_number_of_children_receiving_ART INT 
,	Estimated_number_of_children_needing_ART_methods_median BIGINT
,	Estimated_number_of_children_needing_ART_methods_min BIGINT
,	Estimated_number_of_children_needing_ART_methods_max BIGINT
,	`Estimated_ART_coverage_among_children_(%)_median` BIGINT
,	`Estimated_ART_coverage_among_children_(%)_min` BIGINT
,	`Estimated_ART_coverage_among_children_(%)_max` BIGINT
,	WHO_Region VARCHAR(225) )

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/HIV CASES/Raw files/art_pediatric_coverage_by_country_clean.csv' INTO TABLE pediatric_coverage_by_country
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE DATABASE IF NOT EXISTS cases_adult_15_to_49_by_country;

USE hiv_cases;

CREATE TABLE cases_adult_15_to_49_by_country
( 	Country VARCHAR(225)
,	YEAR INT
,	Count_median BIGINT
,	Count_min BIGINT
,	Count_max BIGINT
,	WHO_Region VARCHAR(225) )

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/HIV CASES/Raw files/no_of_cases_adults_15_to_49_by_country_clean.csv' INTO TABLE cases_adult_15_to_49_by_country
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE DATABASE IF NOT EXISTS death_by_country;

USE hiv_cases;

CREATE TABLE death_by_country
( 	Country VARCHAR(225)
,	YEAR INT
,	Count_median BIGINT
,	Count_min BIGINT
,	Count_max BIGINT
,	WHO_Region VARCHAR(225) )

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/HIV CASES/Raw files/no_of_deaths_by_country_clean.csv' INTO TABLE death_by_country
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE DATABASE IF NOT EXISTS people_living_with_hiv;

USE hiv_cases;

CREATE TABLE people_living_with_hiv
( 	Country VARCHAR(225)
,	YEAR INT
,	Count_median BIGINT
,	Count_min BIGINT
,	Count_max BIGINT
,	WHO_Region VARCHAR(225) )

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/HIV CASES/Raw files/no_of_people_living_with_hiv_by_country_clean.csv' INTO TABLE people_living_with_hiv
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

CREATE DATABASE IF NOT EXISTS prevention_of_mother_to_child;

USE hiv_cases;

CREATE TABLE prevention_of_mother_to_child
( 	Country VARCHAR(225)
,	Received_Antiretrovirals BIGINT 
,	Needing_antiretrovirals_median BIGINT 
,	Needing_antiretrovirals_min BIGINT 
,	Needing_antiretrovirals_max BIGINT 
,	Percentage_Recieved_median BIGINT
,	Percentage_Recieved_min BIGINT 
,	Percentage_Recieved_max BIGINT 
,	WHO_Region VARCHAR(225) )

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/HIV CASES/Raw files/prevention_of_mother_to_child_transmission_by_country_clean.csv' INTO TABLE prevention_of_mother_to_child
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


-- Checking For Duplicate
WITH Dup1 AS(
SELECT 	*
   ,	Row_Number() OVER (PARTITION BY	   Country 
				        ,  Reported_number_of_people_receiving_ART 
				        ,  Estimated_number_of_people_living_with_HIV_median  
					,  Estimated_number_of_people_living_with_HIV_min  
					,  Estimated_number_of_people_living_with_HIV_max 
					,  `Estimated_ART_coverage_among_people_living_with_HIV_(%)_median` 
					,  `Estimated_ART_coverage_among_people_living_with_HIV_(%)_min` 
					,  `Estimated_ART_coverage_among_people_living_with_HIV_(%)_max` 
					,   WHO_Region  ) AS Row_Numb
										
FROM hiv_cases.coverage_by_country )

SELECT *
FROM Dup1
WHERE Row_Numb > 1

WITH Dup2 AS(
SELECT 	*
   ,	Row_Number() OVER (PARTITION BY	     Country 
					  ,  Reported_number_of_children_receiving_ART
					  ,  Estimated_number_of_children_needing_ART_methods_median 
					  ,  Estimated_number_of_children_needing_ART_methods_min 
					  ,  Estimated_number_of_children_needing_ART_methods_max 
				          ,  `Estimated_ART_coverage_among_children_(%)_median`
					  ,  `Estimated_ART_coverage_among_children_(%)_min` 
					  ,  `Estimated_ART_coverage_among_children_(%)_max`
					  ,   WHO_Region  ) AS Row_Numb
FROM hiv_cases.pediatric_coverage_by_country)

SELECT *
FROM Dup2
WHERE Row_Numb > 1

WITH Dup3 AS(
SELECT 	*
   ,	Row_Number() OVER (PARTITION BY	       Country 
					   ,   YEAR 
					   ,   Count_median 
					   ,   Count_min 
					   ,   Count_max 
					   ,   WHO_Region  ) AS Row_Numb
FROM hiv_cases.cases_adult_15_to_49_by_country)

SELECT *
FROM Dup3
WHERE Row_Numb > 1

WITH Dup4 AS(
SELECT 	*
   ,	Row_Number() OVER (PARTITION BY		 Country
					     ,	 YEAR
					     ,	 Count_median
					     ,	 Count_min 
					     ,	 Count_max
					     ,	 WHO_Region) AS Row_Numb
FROM hiv_cases.death_by_country )

SELECT *
FROM Dup4
WHERE Row_Numb > 1

WITH Dup5 AS(
SELECT 	*
   ,	Row_Number() OVER (PARTITION BY		  Country 
					      ,	  YEAR 
					      ,	  Count_median 
					      ,	  Count_min 
					      ,	  Count_max 
					      ,	  WHO_Region ) AS Row_Numb
FROM hiv_cases.people_living_with_hiv)

SELECT *
FROM Dup5
WHERE Row_Numb > 1

WITH Dup6 AS(
SELECT 	*
   ,	Row_Number() OVER (PARTITION BY		    Country 
					        ,   Received_Antiretrovirals 
						,   Needing_antiretrovirals_median 
						,   Needing_antiretrovirals_min  
						,   Needing_antiretrovirals_max 
						,   Percentage_Recieved_median 
						,   Percentage_Recieved_min 
						,   Percentage_Recieved_max 
						,   WHO_Region ) AS Row_Numb
FROM hiv_cases.prevention_of_mother_to_child)

SELECT *
FROM Dup6
WHERE Row_Numb > 1

-- Checking For Null
SELECT *
FROM hiv_cases.coverage_by_country
WHERE 	Country IS NULL
	OR	Reported_number_of_people_receiving_ART IS NULL
	OR	Estimated_number_of_people_living_with_HIV_median IS NULL  
	OR	Estimated_number_of_people_living_with_HIV_min  IS NULL  
	OR	Estimated_number_of_people_living_with_HIV_max IS NULL  
	OR	`Estimated_ART_coverage_among_people_living_with_HIV_(%)_median` IS NULL  
	OR	`Estimated_ART_coverage_among_people_living_with_HIV_(%)_min` IS NULL  
	OR	`Estimated_ART_coverage_among_people_living_with_HIV_(%)_max` IS NULL  
	OR	WHO_Region IS NULL  

SELECT *
FROM hiv_cases.pediatric_coverage_by_country
WHERE 	Country IS NULL
	OR	Reported_number_of_children_receiving_ART IS NULL
	OR	Estimated_number_of_children_needing_ART_methods_median IS NULL
	OR	Estimated_number_of_children_needing_ART_methods_min IS NULL
	OR	Estimated_number_of_children_needing_ART_methods_max IS NULL
	OR	`Estimated_ART_coverage_among_children_(%)_median` IS NULL
	OR	`Estimated_ART_coverage_among_children_(%)_min` IS NULL
	OR	`Estimated_ART_coverage_among_children_(%)_max` IS NULL
	OR	WHO_Region IS NULL
    
SELECT *
FROM hiv_cases.cases_adult_15_to_49_by_country
WHERE 	Country IS NULL
	OR	YEAR IS NULL
	OR	Count_median IS NULL
	OR	Count_min IS NULL
	OR	Count_max IS NULL
	OR	WHO_Region IS NULL

SELECT *
FROM hiv_cases.death_by_country
WHERE 	Country IS NULL
	OR	YEAR IS NULL
	OR	Count_median IS NULL
	OR	Count_min IS NULL
	OR	Count_max IS NULL
	OR	WHO_Region IS NULL

SELECT *
FROM hiv_cases.people_living_with_hiv
WHERE 	Country IS NULL
	OR	YEAR IS NULL
	OR	Count_median IS NULL
	OR	Count_min IS NULL
	OR	Count_max IS NULL
	OR	WHO_Region IS NULL
	
SELECT *
FROM hiv_cases.prevention_of_mother_to_child
WHERE 	Country IS NULL
	OR	Received_Antiretrovirals IS NULL
	OR	Needing_antiretrovirals_median IS NULL
	OR	Needing_antiretrovirals_min  IS NULL
	OR	Needing_antiretrovirals_max IS NULL
	OR	Percentage_Recieved_median IS NULL
	OR	Percentage_Recieved_min IS NULL
	OR	Percentage_Recieved_max IS NULL
	OR	WHO_Region IS NULL

-- Data Manipulation
-- Deaths Per Country Per Year
SELECT 
	Year
,	Country
,	WHO_Region
,	Count_median
,	Count_min
,	Count_max
FROM hiv_cases.death_by_country
ORDER BY YEAR DESC, Country ASC

-- Cases Per Year
SELECT	Country
,	Year
,	Count_median AS Cases
FROM hiv_cases.people_living_with_hiv

-- Children Recieving ART
SELECT 
	Country
,	WHO_Region
,	Reported_number_of_children_receiving_ART AS Repoted_number_of_children_receiving_ART 
,	Estimated_number_of_children_needing_ART_methods_median AS Number_of_children_needing_ART
,	ROUND((Reported_number_of_children_receiving_ART/ Estimated_number_of_children_needing_ART_methods_median) * 100, 2) AS Percentage_of_ART_received 
FROM hiv_cases.pediatric_coverage_by_country

-- People Living With HIV AND People Receiving It
SELECT 
	Country
,	WHO_Region
,	Reported_number_of_people_receiving_ART AS Number_of_people_receiving_ART
,	Estimated_number_of_people_living_with_HIV_median AS Number_of_people_living_with_HIV
,	ROUND((Reported_number_of_people_receiving_ART/ Estimated_number_of_people_living_with_HIV_median ) * 100,2) AS Percentage_of_people_received_ART
FROM hiv_cases.coverage_by_country

-- Mother To Child Transmission
SELECT 
	Country
,	WHO_Region
,	CAST(REPLACE(REPLACE(Received_Antiretrovirals, 'Nodata','0') , ' ','') AS FLOAT) AS Received_Antiretrovirals
,	Needing_antiretrovirals_median AS Needing_antiretrovirals
,	ROUND((CAST(REPLACE(REPLACE(Received_Antiretrovirals, 'Nodata','0') , ' ','') AS FLOAT) / CAST(Needing_antiretrovirals_median AS FLOAT)) * 100,2) AS Percentage_of_received_ART
FROM hiv_cases.prevention_of_mother_to_child

-- Creating A Table With Number of Cases, Living With HIV, Received ART And Death
WITH HIV_DEATHS AS (
SELECT 	DISTINCT(Country)
,	MAX(Count_median) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_median
,	MAX(Count_min) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_min
,	MAX(Count_max) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_max
, 	WHO_Region
FROM hiv_cases.death_by_country
ORDER BY Country ASC )
SELECT 
	L.Country
,	L.Estimated_number_of_people_living_with_HIV_median AS Number_of_people_living_with_HIV
,	L.Reported_number_of_people_receiving_ART AS  Number_of_people_receiving_ART
,	D.Count_median AS Number_of_deaths
,	ROUND((L.Reported_number_of_people_receiving_ART  / L.Estimated_number_of_people_living_with_HIV_median )*100,2) AS Percentage_of_people_received_ART
,	ROUND((D.Count_median  / L.Estimated_number_of_people_living_with_HIV_median )*100,2) AS Percentage_of_deaths
FROM hiv_cases.coverage_by_country AS L
LEFT JOIN HIV_DEATHS AS D
	ON L.Country = D.Country
ORDER BY COUNTRY ASC

-- Total Number Of People Living With HIV, Received ART, Deaths With Percentage Based On WHO Region
WITH HIV_DEATHS AS (
SELECT 	DISTINCT(Country)
,	MAX(Count_median) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_median
,	MAX(Count_min) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_min
,	MAX(Count_max) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_max
, 	WHO_Region
FROM hiv_cases.death_by_country
ORDER BY Country ASC )

SELECT 
	D.WHO_Region
,	SUM(L.Estimated_number_of_people_living_with_HIV_median) AS Total_number_of_people_living_with_HIV
,	SUM(L.Reported_number_of_people_receiving_ART) AS Total_number_of_received_ART
,	SUM(D.Count_median) AS Total_number_of_deaths
,	(SUM(L.Reported_number_of_people_receiving_ART)  / SUM(L.Estimated_number_of_people_living_with_HIV_median))*100 AS Percentage_of_people_received_ART
,	(SUM(D.Count_median)  / SUM(L.Estimated_number_of_people_living_with_HIV_median))*100 AS Percentage_of_People_Died
FROM hiv_cases.coverage_by_country AS L
JOIN HIV_DEATHS AS D
	ON L.Country = D.Country
GROUP BY D.WHO_Region
ORDER BY Total_number_of_people_living_with_HIV DESC


-- Top 5 Highest Country With People Living With HIV In 2018
SELECT 
	Country
,	Year
,	Who_Region
,	MAX(Count_median) OVER (PARTITION BY COUNTRY)  AS Number_of_people_with_HIV
FROM hiv_cases.people_living_with_hiv
WHERE YEAR = 2018 
ORDER BY Count_median DESC
LIMIT 5

-- Top 5 Received ART 
SELECT 	Country
,	Reported_number_of_people_receiving_ART AS people_received_ART
 FROM hiv_cases.coverage_by_country   
 ORDER BY people_received_ART DESC
 LIMIT 5

    
-- TOP 5 Highest Deaths Per Country IN 2018
SELECT
	Country
,	year
,	Who_Region
,	Count_median AS Number_of_death
FROM hiv_cases.death_by_country
WHERE Year = 2018
ORDER BY Count_median DESC
LIMIT 5

-- Total Deaths Per Year
SELECT 
	Year
,	SUM( Count_median) AS Number_deaths
FROM hiv_cases.death_by_country
GROUP BY YEAR
ORDER BY Year ASC

-- People Living with HIV, Percentage with ART and Percentage Died with HIV In The World
WITH HIV_DEATHS AS (
SELECT 	DISTINCT(Country)
,	MAX(Count_median) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_median
,	MAX(Count_min) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_min
,	MAX(Count_max) OVER (PARTITION BY Country ORDER BY COUNTRY ASC) AS Count_max
, 	WHO_Region
FROM hiv_cases.death_by_country
ORDER BY Country ASC )
	
SELECT	SUM(A.Estimated_number_of_people_living_with_HIV_median) AS People_living_with_HIV
,	(SUM(A.Reported_number_of_people_receiving_ART) / SUM(A.Estimated_number_of_people_living_with_HIV_median))*100 AS Percentage_with_ART
,	(SUM(B.Count_median) / SUM(A.Estimated_number_of_people_living_with_HIV_median))*100 AS Death_Percentage
FROM hiv_cases.coverage_by_country A
INNER JOIN HIV_DEATHS B
	ON A.Country = B.Country









	





		




	



