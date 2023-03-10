#Eshaan Vora
#Data Analyst Case study
#11/25/22
#This file contains the SQL commands used to answer the Case Study questions

USE DataAnalyst_CaseStudy;

# Question 2: Distinct Locations
SELECT DISTINCT UPPER(RESPONSE) as DistinctLocations
FROM Survey
WHERE QUESTION_CODE = 'location';

# Question 3: Surveys per Location
SELECT RESPONSE, COUNT(*)
FROM Survey
WHERE QUESTION_CODE = 'location'
GROUP BY RESPONSE;
# Question 3: Surveys per time of the day
SELECT RESPONSE, COUNT(RESPONSE)
FROM Survey
WHERE QUESTION_CODE = 'time_of_the_day'
GROUP BY RESPONSE;

# Question 3A: Trip Type Ratio by Location
SELECT t2.RESPONSE AS location, t1.RESPONSE AS trip_type, COUNT(*) AS Num_Users
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'q1'
GROUP BY location, t1.RESPONSE;

# Question 3: Surveys by Time of Day and Location
SELECT t1.RESPONSE as time, t2.RESPONSE AS location, COUNT(*)
FROM Survey as t1
INNER JOIN (SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') as t2 ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'time_of_the_day'
GROUP BY location, time;

# Question3: Surveys by Buyer Status and Location
SELECT t2.RESPONSE AS location, COUNT(*) as Number_of_Buyers
FROM Survey as t1
INNER JOIN (SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') as t2 ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'q3' AND t1.RESPONSE = 'i was able to buy everything on my list'
GROUP BY location;

# Question 3B: Time_of_Day Filter for Trip Type by Location
# Question 3B: Buyer/Non-Buyer Filter for Trip Type by Location
SELECT t3.SURVEY_ID, t3.USER_ID, t3.SURVEY_RESPONDED_DATE, RESPONSE1, 
RESPONSE2, t3.RESPONSE AS RESPONSE3
FROM Survey as t3
INNER JOIN (SELECT t1.USER_ID, t1.SURVEY_ID, t1.SURVEY_RESPONDED_DATE, t1.RESPONSE AS RESPONSE1, t2.RESPONSE AS RESPONSE2
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'q1') as t4 
ON t3.USER_ID = t4.USER_ID 
AND t3.SURVEY_RESPONDED_DATE = t4.SURVEY_RESPONDED_DATE
AND t3.SURVEY_ID = t4.SURVEY_ID
WHERE t3.QUESTION_CODE = 'q3';
#WHERE t3.QUESTION_CODE = 'time_of_the_day';

# QUESTION 4: USERS WHO COMPLETED 2 OR MORE SURVEYS
SELECT USER_ID, COUNT(*) AS Number_Of_Surveys
FROM Survey
WHERE QUESTION_CODE = "location"
GROUP BY USER_ID
HAVING Number_Of_Surveys >= 2
ORDER BY Number_Of_Surveys DESC;

#QUESTION 5: VISUALIZE Q2 BY LOCATION AND BUYERS
SELECT t3.SURVEY_ID, t3.USER_ID, t3.SURVEY_RESPONDED_DATE, RESPONSE1, 
RESPONSE2, t3.RESPONSE AS RESPONSE3, t3.PRODUCT_CATEGORY AS PRODUCT
FROM Survey as t3
INNER JOIN (SELECT t1.USER_ID, t1.SURVEY_ID, t1.SURVEY_RESPONDED_DATE, t1.RESPONSE AS RESPONSE1, t2.RESPONSE AS RESPONSE2
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'q3') as t4 
ON t3.USER_ID = t4.USER_ID 
AND t3.SURVEY_RESPONDED_DATE = t4.SURVEY_RESPONDED_DATE
AND t3.SURVEY_ID = t4.SURVEY_ID
WHERE t3.QUESTION_CODE = 'q2';

# QUESTION 6: RECOMMENDATION PER LOCATION
SELECT t2.RESPONSE AS Location, t1.RESPONSE AS Recommendation, Count(*) AS Num_Surveys
FROM Survey as t1
INNER JOIN (SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') as t2 ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'q4'
GROUP BY Location, Recommendation;

# QUESTION 6: RECOMMENDATION SCORE CALCULATION
SELECT Location, SUM(Num_Surveys)*100 AS Recommendation_Score
FROM (
SELECT Location, Recommendation, 
IF(Recommendation = "no", -1*Num_Surveys, Num_Surveys) AS Num_Surveys
FROM (
SELECT t2.RESPONSE AS Location, t1.RESPONSE AS Recommendation, Count(*) AS Num_Surveys
FROM Survey as t1
INNER JOIN (SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.QUESTION_CODE = 'q4'
GROUP BY Location, Recommendation
) as ModifiedSurvey
) as Survey
WHERE Recommendation != "maybe"
GROUP BY Location
ORDER BY Recommendation_Score DESC;

# QUESTION 7: Most Important Purchasing Factor among Walmart Buyers
SELECT RESPONSE2 AS Location, t3.RESPONSE as Purchasing_Factor, COUNT(*) AS Number_Of_Buyers
FROM Survey as t3
INNER JOIN (SELECT t1.USER_ID, t1.SURVEY_ID, t1.SURVEY_RESPONDED_DATE, t1.RESPONSE AS RESPONSE1, t2.RESPONSE AS RESPONSE2
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE RESPONSE = 'walmart') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.RESPONSE = 'i was able to buy everything on my list') as t4 
ON t3.USER_ID = t4.USER_ID 
AND t3.SURVEY_RESPONDED_DATE = t4.SURVEY_RESPONDED_DATE
AND t3.SURVEY_ID = t4.SURVEY_ID
WHERE t3.QUESTION_CODE = 'q2'
GROUP BY Purchasing_Factor
ORDER BY Number_Of_Buyers DESC;

# QUESTION 8: Top 2 locations among buyers who purchased Coffee/Tea
SELECT RESPONSE2 AS Location, COUNT(*) AS Coffee_Buyers
FROM Survey as t3
INNER JOIN (SELECT t1.USER_ID, t1.SURVEY_ID, t1.SURVEY_RESPONDED_DATE, t1.RESPONSE AS RESPONSE1, t2.RESPONSE AS RESPONSE2
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.RESPONSE = 'i was able to buy everything on my list') as t4 
ON t3.USER_ID = t4.USER_ID 
AND t3.SURVEY_RESPONDED_DATE = t4.SURVEY_RESPONDED_DATE
AND t3.SURVEY_ID = t4.SURVEY_ID
WHERE t3.PRODUCT_CATEGORY = 'Coffee/Tea'
GROUP BY Location
ORDER BY Coffee_Buyers DESC
LIMIT 2;

# QUESTION 8: Most important factor driving the Coffee purchase AMONG BUYERS
SELECT RESPONSE AS Purchasing_Factor, COUNT(*) AS Coffee_Buyers
FROM Survey as t3
INNER JOIN (SELECT t1.USER_ID, t1.SURVEY_ID, t1.SURVEY_RESPONDED_DATE, t1.RESPONSE AS RESPONSE1, t2.RESPONSE AS RESPONSE2
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.RESPONSE = 'i was able to buy everything on my list') as t4 
ON t3.USER_ID = t4.USER_ID 
AND t3.SURVEY_RESPONDED_DATE = t4.SURVEY_RESPONDED_DATE
AND t3.SURVEY_ID = t4.SURVEY_ID
WHERE t3.PRODUCT_CATEGORY = 'Coffee/Tea'
GROUP BY Purchasing_Factor
ORDER BY Coffee_Buyers DESC
LIMIT 1;

# QUESTION 8: Factors driving the Coffee purchase AMONG ALL USERS 
SELECT RESPONSE AS Purchasing_Factor, COUNT(*) AS Coffee_Buyers
FROM Survey
WHERE PRODUCT_CATEGORY = 'Coffee/Tea'
GROUP BY Purchasing_Factor;

# QUESTION 9: Most preferred location for Grab & Go is WALMART
SELECT t2.RESPONSE AS location, t1.RESPONSE AS trip, COUNT(*) as Num_Users
FROM Survey as t1
INNER JOIN 
(SELECT *
FROM Survey
WHERE QUESTION_CODE = 'location') AS t2 
ON t1.USER_ID = t2.USER_ID 
AND t1.SURVEY_RESPONDED_DATE = t2.SURVEY_RESPONDED_DATE 
AND t1.SURVEY_ID = t2.SURVEY_ID
WHERE t1.RESPONSE = 'grab & go'
GROUP BY location
ORDER BY Num_Users DESC
LIMIT 2;