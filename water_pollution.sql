-- DUPLICATE THE DATA FOR SAFE EDITING
CREATE DATABASE IF NOT EXISTS Water_Pollution_dup;

USE water_pollution;

CREATE TABLE Water_Pollution_dup
(	Country VARCHAR(255)
,	Region VARCHAR(255)
,	Year	INT
,	Water_Source_Type VARCHAR(255)
,	Contaminant_Level_ppm FLOAT
,	pH_Level FLOAT
,	`Turbidity_(NTU)` FLOAT
,	`Dissolved_Oxygen_(mg/L)` FLOAT
,	`Nitrate_Level_(mg/L)` FLOAT
,	`Lead_Concentration_(Âµg/L)` FLOAT
,	`Bacteria_Count_(CFU/mL)` INT
,	Water_Treatment_Method VARCHAR(255)
,	`Access_to_Clean_Water_(%_of_Population)` FLOAT
,	`Diarrheal_Cases_per_100,000_people` INT
,	`Cholera_Cases_per_100,000_people` INT
,	`Typhoid_Cases_per_100,000_people` INT
,	`Infant_Mortality_Rate_(per_1,000_live_births)` FLOAT
,	`GDP_per_Capita_(USD)` INT
,	`Healthcare_Access_Index_(0-100)` FLOAT 
,	`Urbanization_Rate_(%)` FLOAT
,	`Sanitation_Coverage_(%_of_Population)` FLOAT
,	`Rainfall_(mm_per_year)` INT
,	`Temperature_(Â°C)` FLOAT
,	`Population_Density_(people_per_kmÂ²)` INT 
)

SET GLOBAL LOCAL_INFILE = ON ;
LOAD DATA LOCAL INFILE 'D:/Data Analyst/Data analyst/PROJECTS/Water Polution/water_pollution_disease.csv' INTO TABLE Water_Pollution_dup
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Data Cleaning
-- Checking For Duplicates
WITH dup AS (
SELECT *
, Row_Number() OVER ( PARTITION BY  Country
							,	Region
							,	Water_Source_Type
                            ,	Contaminant_Level_ppm
                            ,	pH_Level
                            ,	`Turbidity_(NTU)`
                            ,	`Dissolved_Oxygen_(mg/L)`
							,	`Nitrate_Level_(mg/L)`
                            ,	`Lead_Concentration_(Âµg/L)`
                            ,	`Bacteria_Count_(CFU/mL)`
                            ,	Water_Treatment_Method
                            ,	`Access_to_Clean_Water_(%_of_Population)`
                            ,	`Diarrheal_Cases_per_100,000_people`
                            ,	`Cholera_Cases_per_100,000_people`
                            ,	`Typhoid_Cases_per_100,000_people`
                            ,	`Infant_Mortality_Rate_(per_1,000_live_births)`
                            ,	`GDP_per_Capita_(USD)`,`Healthcare_Access_Index_(0-100)`
                            ,	`Urbanization_Rate_(%)`,`Sanitation_Coverage_(%_of_Population)`
                            ,	`Rainfall_(mm_per_year)`,`Temperature_(Â°C)`
                            ,	`Population_Density_(people_per_kmÂ²)` ) AS row_num
FROM water_pollution.water_pollution_dup )

SELECT *
FROM dup
WHERE row_num > 1

-- Data Manipulation
-- Highest Contamination Level Per Country
SELECT 	Country
	,	Region
    ,	Water_Source_Type
    ,	Contaminant_Level_ppm
FROM water_pollution.water_pollution_dup
ORDER BY Contaminant_Level_ppm DESC

-- Highest Contamination Level Based on Water Source
SELECT	Water_Source_Type
    ,	MAX(Contaminant_Level_ppm) AS max_contamination_Level
FROM water_pollution.water_pollution_dup
GROUP BY Water_Source_Type
ORDER BY max_contamination_Level DESC

-- Highest Diarrhea cases per 100,000  Per Year
SELECT 
		Country
   ,	 Year
   ,	 Water_Source_Type
   ,	 `Diarrheal_Cases_per_100,000_people`
FROM (
		   SELECT *,
           RANK() OVER (PARTITION BY Year ORDER BY `Diarrheal_Cases_per_100,000_people` DESC) AS rnk
		   FROM water_pollution.water_pollution_dup
) t
WHERE rnk = 1
ORDER BY Year DESC, `Diarrheal_Cases_per_100,000_people` DESC

-- Highest Cholera  Cases Per 100,000 People Per Year
SELECT 
		Country
   ,	 Year
   ,	 Water_Source_Type
   ,	 `Cholera_Cases_per_100,000_people`
FROM (
		   SELECT * ,
           RANK() OVER (PARTITION BY Year ORDER BY `Cholera_Cases_per_100,000_people` DESC) AS rnk
		   FROM water_pollution.water_pollution_dup
) t
WHERE rnk = 1
ORDER BY Year DESC


-- Highest Typoid Cases Per 100,000 People  Year
SELECT	Country
,		Year
,		Water_Source_Type
,		`Typhoid_Cases_per_100,000_people`
FROM 	(SELECT *
			,	RANK() OVER (PARTITION BY YEAR ORDER BY `Typhoid_Cases_per_100,000_people` DESC ) AS rnk
		FROM water_pollution.water_pollution_dup ) t
        
WHERE rnk = 1
ORDER BY YEAR DESC 


-- Checking For The Water Quality 
SELECT	Country
	,	Region
    ,	Year
    ,	Water_Source_Type
    ,	Contaminant_Level_ppm
    ,	pH_Level
    ,	`Turbidity_(NTU)`
    ,	`Dissolved_Oxygen_(mg/L)`
    ,	`Nitrate_Level_(mg/L)`
    ,	`Lead_Concentration_(Âµg/L)`
    ,	`Bacteria_Count_(CFU/mL)`
    ,	CASE
			WHEN Contaminant_Level_ppm < 500 
					AND pH_Level BETWEEN 6.5 AND 8
                    AND `Turbidity_(NTU)` < 5
                    AND `Dissolved_Oxygen_(mg/L)` BETWEEN 6.5 AND 8
                    AND `Nitrate_Level_(mg/L)` < 10 
                    AND `Lead_Concentration_(Âµg/L)` < 15
                    AND  `Bacteria_Count_(CFU/mL)` < 100 THEN ' Drinkable Water'
                    ELSE ' Not Drinkable Water'
                    END AS Water_Quality
FROM water_pollution.water_pollution_dup
ORDER BY YEAR DESC, Country ASC

-- Mortality Rate Of Infant Caused By High Nitrate Level
SELECT	Country
	,	Region
    ,	Year
    ,	Water_Source_Type
    ,	`Typhoid_Cases_per_100,000_people`
    ,	`Access_to_Clean_Water_(%_of_Population)`
    ,	Water_Treatment_Method
    ,	`Nitrate_Level_(mg/L)`
    ,	CASE
			WHEN `Nitrate_Level_(mg/L)` < 10  THEN 'Safe for Infants'
            ELSE 'Not Safe for Infants'
            END AS Water_Quality    
FROM water_pollution.water_pollution_dup
ORDER BY YEAR DESC, Country ASC , Region ASC

-- The Urbanization Rate In Relation To The Causes Of Water Quality Issues.
SELECT	Country
	,	Region
    ,	Year
    ,	Water_Source_Type
	,	CASE
			WHEN Contaminant_Level_ppm < 500 
					AND pH_Level < 8 AND pH_Level > 6.5
                    AND `Turbidity_(NTU)` < 5
                    AND `Dissolved_Oxygen_(mg/L)` > 6.5 AND `Dissolved_Oxygen_(mg/L)` < 8
                    AND `Nitrate_Level_(mg/L)` < 10 
                    AND `Lead_Concentration_(Âµg/L)` < 15
                    AND  `Bacteria_Count_(CFU/mL)` < 100 THEN ' Drinkable Water'
                    ELSE ' Not Drinkable Water'
                    END AS Water_Quality
    ,	`Urbanization_Rate_(%)`
    ,	`Access_to_Clean_Water_(%_of_Population)`
    ,	`Diarrheal_Cases_per_100,000_people`
    ,	`Cholera_Cases_per_100,000_people`
	,	`Typhoid_Cases_per_100,000_people`
    ,	`Infant_Mortality_Rate_(per_1,000_live_births)`
FROM water_pollution.water_pollution_dup
ORDER BY YEAR DESC, Country ASC, Region ASC

-- Healthcare Access That Contributes To The Rise Of Diseases.
SELECT	Country
	,	Region
    ,	Year
    ,	Water_Source_Type
    ,	`Healthcare_Access_Index_(0-100)`
    ,	`Sanitation_Coverage_(%_of_Population)`
    ,	`Access_to_Clean_Water_(%_of_Population)`
FROM water_pollution.water_pollution_dup
ORDER BY  Year DESC, Country ASC


-- Dangerous Water Source Type From  2000 To 2024
SELECT	 Water_Source_Type
	,	ROUND(AVG(Contaminant_Level_ppm) , 2) AS AVG_Contaminant_Level
    ,	CASE
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500 THEN ' Contaminant Level is Safe'
            ELSE 'Contaminant Level is Not Safe'
            END AS Contaminant_Level_Reading
    ,	ROUND(AVG(pH_Level), 2) AS AVG_pH_Level
	,	CASE
			WHEN ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5 THEN ' pH Level is Safe'
            ELSE 'pH Level is Safe'
            END AS pH_Level_Reading
    ,	ROUND(AVG(`Turbidity_(NTU)`), 2) AS AVG_Turbidity
    ,	CASE
			WHEN ROUND(AVG(`Turbidity_(NTU)`), 2) < 5 THEN ' Turbidity Level is Safe'
            ELSE 'Turbidity Level is Not Safe'
            END AS Turbidity_Reading
    ,	ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) AS AVG_Dissolved_Oxygen
    ,	CASE
			WHEN ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8 THEN 'Dissolved_Oxygen level is Safe'
            ELSE 'Dissolved_Oxygen level is Not Safe'
            END AS Dissolved_Oxygen_Reading
    ,	ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) AS AVG_Nitrate_Level
    ,	CASE
			WHEN ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10 THEN 'Nitrate level is Safe'
            ELSE 'Nitrate level is Not Safe'
            END AS Nitrate_Level_Reading
    ,	ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) AS AVG_Lead_Concentration
    ,	CASE
			WHEN ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15 THEN 'Lead Concentration is Safe'
            ELSE 'Lead Concentration is Not Safe'
            END AS Lead_Concentration_Reading
    ,	ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) AS AVG_Bacteria_Count
    ,	CASE
			WHEN ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Bacteria Count is Safe'
            ELSE 'Bacteria Count is Not Safe'
            END AS Bacteria_Count_Reading
    ,	CASE 
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Drinkable Water'
                ELSE 'Not Drinkable Water'
                END AS Overall_Water_Quality
FROM water_pollution.water_pollution_dup
GROUP BY Water_Source_Type
ORDER BY 	Water_Source_Type	DESC
		,	AVG_Contaminant_Level DESC
		, 	AVG_pH_Level DESC
        , 	AVG_Turbidity DESC
        , 	AVG_Dissolved_Oxygen DESC
        , 	AVG_Nitrate_Level DESC
        , 	AVG_Lead_Concentration DESC
        , 	AVG_Bacteria_Count DESC 

-- Water Treatment Method Used From 2000 to 2024
SELECT	Water_Treatment_Method
    ,	ROUND(AVG(`Diarrheal_Cases_per_100,000_people`),2) AS AVG_Diarreal_Cases
    ,	ROUND(AVG(`Cholera_Cases_per_100,000_people`), 2) AS AVG_Cholera_Cases
    ,	ROUND(AVG(`Typhoid_Cases_per_100,000_people`),2) AS AVG_Typhoid_Cases
    ,	ROUND(AVG(`Infant_Mortality_Rate_(per_1,000_live_births)`),2 ) AS AVG_Infant_Mortality_Rate
	,	ROUND(AVG(Contaminant_Level_ppm) , 2) AS AVG_Contaminant_Level
    ,	CASE
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500 THEN ' Contaminant Level is Safe'
            ELSE 'Contaminant Level is Not Safe'
            END AS Contaminant_Level_Reading
    ,	ROUND(AVG(pH_Level), 2) AS AVG_pH_Level
    ,	CASE
			WHEN ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5 THEN ' pH Level is Safe'
            ELSE 'pH Level is Safe'
            END AS pH_Level_Reading
    ,	ROUND(AVG(`Turbidity_(NTU)`), 2) AS AVG_Turbidity
    ,	CASE
			WHEN ROUND(AVG(`Turbidity_(NTU)`), 2) < 5 THEN ' Turbidity Level is Safe'
            ELSE 'Turbidity Level is Not Safe'
            END AS Turbidity_Reading
    ,	ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) AS AVG_Dissolved_Oxygen
    ,	CASE
			WHEN ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8 THEN 'Dissolved_Oxygen level is Safe'
            ELSE 'Dissolved_Oxygen level is Not Safe'
            END AS Dissolved_Oxygen_Reading
    ,	ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) AS AVG_Nitrate_Level
	,	CASE
			WHEN ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10 THEN 'Nitrate level is Safe'
            ELSE 'Nitrate level is Not Safe'
            END AS Nitrate_Level_Reading
    ,	ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) AS AVG_Lead_Concentration
    ,	CASE
			WHEN ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15 THEN 'Lead Concentration is Safe'
            ELSE 'Lead Concentration is Not Safe'
            END AS Lead_Concentration_Reading
    ,	ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) AS AVG_Bacteria_Count
    ,	CASE
			WHEN ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Bacteria Count is Safe'
            ELSE 'Bacteria Count is Not Safe'
            END AS Bacteria_Count_Reading
    , 	CASE
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Drinkable Water'
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) > 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Contaminant Level is not safe'
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) > 8 AND  ROUND(AVG(pH_Level), 2) < 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'pH level is not safe'
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) > 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Turbidity Level is not safe'   
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Dissoleved Oxygen Level is not safe'   
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) > 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) < 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Nitrate Level is not safe'  
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) > 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) < 100 THEN 'Lead Concentration Level is not safe'  
			WHEN ROUND(AVG(Contaminant_Level_ppm) , 2) < 500
				AND ROUND(AVG(pH_Level), 2) < 8 AND  ROUND(AVG(pH_Level), 2) > 6.5
                AND ROUND(AVG(`Turbidity_(NTU)`), 2) < 5
                AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) > 6.5 AND ROUND(AVG(`Dissolved_Oxygen_(mg/L)`), 2) < 8
                AND ROUND(AVG(`Nitrate_Level_(mg/L)`), 2) < 10
                AND ROUND(AVG(`Lead_Concentration_(Âµg/L)`), 2) > 15
                AND ROUND(AVG(`Bacteria_Count_(CFU/mL)`),2 ) > 100 THEN 'Bacteria Count is not safe'  
		ELSE 'Water is not safe'
        END AS Overall_Water_Quality
FROM water_pollution.water_pollution_dup
GROUP BY Water_Treatment_Method
ORDER BY	
			 AVG_Cholera_Cases DESC
        ,	 AVG_Typhoid_Cases DESC
        ,	 AVG_Infant_Mortality_Rate DESC


-- Water Treatment Method Used In Different Types Of Water Source
WITH water_treatment AS (
SELECT	DISTINCT(Water_Source_Type) AS Water_Source_Type
	,	Water_Treatment_Method
    ,	COUNT(Water_Source_Type)  OVER (PARTITION BY Water_Treatment_Method,Water_Source_Type) AS Count_Water_Treatment_Method_Used
FROM water_pollution.water_pollution_dup )

SELECT	Water_Source_Type
	,	Water_Treatment_Method
    ,	ROUND(Count_Water_Treatment_Method_Used / SUM(Count_Water_Treatment_Method_Used) OVER (PARTITION BY Water_Treatment_Method) * 100, 2) AS Percentage_Water_Treatment
FROM water_treatment


-- The Most Used Water Treatment In 2024
SELECT 	DISTINCT(Water_Treatment_Method)
	,	ROUND((COUNT(Water_Treatment_Method) OVER (PARTITION BY Water_Treatment_Method)  / COUNT(Water_Treatment_Method) OVER ())*100  , 2) AS Percentage
FROM water_pollution.water_pollution_dup
WHERE YEAR = 2024
ORDER BY Percentage DESC

-- Cases Of Diseases From 2000 To 2024
SELECT 	Country
	,	SUM(`Diarrheal_Cases_per_100,000_people`) AS Diarrheal_Cases
    ,	SUM(`Cholera_Cases_per_100,000_people`) AS Cholera_Case
    ,	SUM(`Typhoid_Cases_per_100,000_people`) AS Typhoid_Cases
FROM water_pollution.water_pollution_dup
GROUP BY Country
ORDER BY Country ASC

