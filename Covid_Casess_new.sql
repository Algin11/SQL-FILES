-- Creating Table For Data Manipulation
CREATE DATABASE IF NOT EXISTS Covid_Deaths;

USE Covid_Cases;

CREATE TABLE Covid_Deaths
(
	iso_code VARCHAR(255)
, 	continent VARCHAR(255)
,	location VARCHAR(255)
,	population BIGINT
,	date VARCHAR(20)
,	total_cases INT
,	new_cases INT
,	new_cases_smoothed INT
,	total_deaths INT
,	new_deaths INT
,	new_deaths_smoothed INT )
            
SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/COVID CASES/raw files/CovidDeaths.csv' INTO TABLE covid_deaths
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


CREATE DATABASE IF NOT EXISTS Covid_Vaccinations;

USE Covid_Cases;

CREATE TABLE Covid_Vaccinations
(
	iso_code VARCHAR(255)
, 	continent VARCHAR(255)
,	location VARCHAR(255)
,	population BIGINT
,	date VARCHAR(20)
,	total_tests BIGINT
,	new_tests BIGINT
,	positive_rate BIGINT
,	tests_per_case BIGINT
,	total_vaccinations BIGINT
,	people_vaccinated BIGINT
,	people_fully_vaccinated BIGINT
,	total_boosters BIGINT 
,	new_vaccinations BIGINT
,	extreme_poverty FLOAT 
,	diabetes_prevalence FLOAT 
,	female_smokers FLOAT
,	male_smokers FLOAT )

LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/COVID CASES/raw files/CovidVaccination.csv' INTO TABLE covid_vaccinations
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


-- Checking For Duplicate
WITH DUP1 AS (
SELECT 	*
,	ROW_NUMBER() OVER (PARTITION BY         iso_code 
					, 	continent
					,	location 
					,	population 
					,	date 
					,	total_cases 
					,	new_cases 
					,	new_cases_smoothed 
					,	total_deaths 
					,	new_deaths 
					,	new_deaths_smoothed )  AS Row_Numb
                                        
FROM covid_cases.covid_deaths )

SELECT *
FROM DUP1
WHERE Row_Numb > 1

WITH DUP2 AS(
SELECT	*
	,	ROW_NUMBER() OVER (PARTITION BY  iso_code
	, 	continent 
	,	location 
	,	population 
	,	date 
	,	total_tests 
	,	new_tests 
	,	positive_rate
	,	tests_per_case 
	,	total_vaccinations 
	,	people_vaccinated 
	,	people_fully_vaccinated 
	,	total_boosters  
	,	new_vaccinations 
	,	extreme_poverty 
	,	diabetes_prevalence
	,	female_smokers
	,	male_smokers ) AS Row_Numb
FROM covid_cases.covid_vaccinations )

SELECT *
FROM DUP2 
WHERE Row_Numb > 1

-- Cheking For Null
SELECT *
FROM covid_cases.covid_deaths
WHERE	iso_code IS NULL
	OR 	continent IS NULL
	OR	location IS NULL 
	OR	population IS NULL 
	OR	date IS NULL
	OR 	total_cases IS NULL
	OR 	new_cases IS NULL
	OR 	new_cases_smoothed IS NULL
	OR 	total_deaths IS NULL
	OR 	new_deaths IS NULL
	OR 	new_deaths_smoothed IS NULL

SELECT *
FROM covid_cases.covid_vaccinations
WHERE	iso_code IS NULL
	OR	continent IS NULL 
	OR 	location IS NULL 
	OR 	population IS NULL 
	OR 	date IS NULL 
	OR 	total_tests IS NULL 
	OR 	new_tests IS NULL 
	OR 	positive_rate IS NULL
	OR 	tests_per_case IS NULL 
	OR	total_vaccinations IS NULL 
	OR 	people_vaccinated IS NULL
	OR	people_fully_vaccinated IS NULL
	OR 	total_boosters IS NULL  
	OR 	new_vaccinations IS NULL 
	OR 	extreme_poverty IS NULL
	OR 	diabetes_prevalence IS NULL
	OR 	female_smokers IS NULL
	OR 	male_smokers IS NULL
	
-- Data exploration/ Manipulation
-- Changing the Date To The Exact Format
Update covid_cases.covid_deaths
SET date = STR_TO_DATE(date, '%d/%m/%Y')

ALTER TABLE covid_cases.covid_deaths
MODIFY date DATE

Update covid_cases.covid_vaccinations
SET date = STR_TO_DATE(date, '%d/%m/%Y')

ALTER TABLE covid_cases.covid_vaccinations
MODIFY date DATE;

-- Table For Infection Rate, Death And People Vaccianted
SELECT	DISTINCT(D.location)
    ,	D.population
    ,	MAX(D.total_cases) OVER (PARTITION BY D.location) AS total_cases
    ,	MAX(D.total_deaths) OVER (PARTITION BY D.location) AS total_deaths
    ,	MAX(V.people_vaccinated) OVER (PARTITION BY location) AS people_vaccinated
FROM covid_cases.covid_deaths D
INNER JOIN covid_cases.covid_vaccinations V
	on D.location = V.location AND D.date = V.date
ORDER BY D.location ASC 

-- Table For Total Cases With Percentage
SELECT	location
    ,	date
    ,   population
    ,	new_cases
    ,	total_cases
    ,	(total_cases/population)*100  AS cases_percentage
FROM covid_cases.Covid_Deaths
ORDER BY 1,2

-- Table For Total Cases And Total Deaths With Percentage
SELECT	location
     ,	date
     ,	total_cases
     ,	new_cases
     ,	total_deaths
     ,	(total_deaths/total_cases)*100  AS death_percentage
FROM covid_cases.Covid_Deaths
ORDER BY 1,2

-- Countries With Highest Infection Rate Compared To Population
SELECT	location
     ,	population
     ,	MAX(total_cases) AS covid_cases
     ,	(MAX(total_cases)/population)*100  AS infection_rate
FROM covid_cases.Covid_Deaths
GROUP BY location, population
ORDER BY infection_rate DESC

-- Countries With The Highest Covid Cases
SELECT	location
     ,	population
     ,	MAX(total_cases) AS covid_cases
     ,	(MAX(total_cases)/population)*100  AS infection_rate
FROM covid_cases.Covid_Deaths
GROUP BY location, population
ORDER BY covid_cases DESC

-- Countries With Highest Death Counts
SELECT	location
     ,	MAX(total_deaths) AS total_deaths
FROM covid_cases.Covid_Deaths
GROUP BY location
ORDER BY total_deaths DESC


-- Death Counts Per Continent
SELECT	continent
     ,	MAX(total_deaths) AS total_deaths
FROM covid_cases.Covid_Deaths
GROUP BY continent
ORDER BY total_deaths DESC

-- Global Numbers
	
WITH global_numbers AS (
    SELECT  DISTINCT(D.continent) AS continent
        ,   MAX(D.population)  OVER (PARTITION BY D.location) AS Population 
        ,   MAX(D.total_cases) OVER (PARTITION BY D.location) AS Total_Cases
        ,   MAX(D.total_deaths)  OVER (PARTITION BY D.location) AS Total_Deaths
        ,   MAX(V.people_vaccinated)  OVER (PARTITION BY V.location) AS People_Vaccinated
    FROM covid_cases.Covid_Deaths D
    INNER JOIN covid_cases.Covid_Vaccinations V
        ON D.location = V.location AND D.date = V.date

)

SELECT  DISTINCT(SUM(Population) OVER()) AS Population
    ,   SUM(Total_Cases) OVER() AS Total_Cases
    ,   SUM(Total_Deaths) OVER() AS Total_Deaths
    ,   SUM(People_Vaccinated) OVER() AS People_Vaccinated
    ,   SUM(Total_Deaths) OVER() / SUM(Population) OVER() * 100 AS Percentage
FROM global_numbers;

-- People Vaccianted
SELECT	D.continent
    ,	D.location
    ,	D.date
    ,	D.population
    ,	V.new_vaccinations
    ,	SUM(V.new_vaccinations) OVER (PARTITION BY D.location ORDER BY D.location,D.date ) AS rolling_people_vaccination
FROM  covid_cases.Covid_Deaths D
JOIN covid_cases.Covid_Vaccinations V	
	ON D.location = V.location AND D.date = V.date
ORDER BY 2,3

-- People Vaccianted With Percentage Using CTE
WITH PopvsVac 
AS (
	SELECT	D.continent
	  ,	D.location
	  ,	D.date
	  ,	D.population
	  ,	V.new_vaccinations
	  ,	SUM(V.new_vaccinations) OVER (PARTITION BY D.location ORDER BY D.location,D.date ) AS rolling_people_vaccination
FROM covid_cases.Covid_Deaths D
JOIN covid_cases.Covid_Vaccinations V	
	ON D.location = V.location
	and D.date = V.date
  )

SELECT * 
  , 	(rolling_people_vaccination/Population)*100 AS RollingVaccinated_Percentage
FROM PopvsVac


-- Vaccianted Percentage Per Country Using Temp Table
CREATE DATABASE IF NOT EXISTS PopulationVaccinated ;
USE Covid_Cases;

CREATE TABLE PopulationVaccinated
(	Continent varchar(255)
  ,	Location varchar(255)
  ,	Date date
  ,	Population INT
  ,	New_Vaccination INT
  ,	RollingVaccinated BIGINT )

INSERT INTO PopulationVaccinated
SELECT	D.continent
  ,	D.location
  ,	D.date
  ,	D.population
  ,	V.new_vaccinations
  ,	SUM(V.new_vaccinations) OVER (PARTITION BY D.location ORDER BY D.location,D.date ) 
FROM covid_cases.Covid_Deaths D
JOIN covid_cases.Covid_Vaccinations V	
	ON D.location = V.location
	and D.date = V.date

SELECT * 
, 
(RollingVaccinated/Population)*100 AS RollingVaccinated_Percentage
FROM PopulationVaccinated























    

    


