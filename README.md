# Jahnavi Maddhuri: Complex SQL Query with a Databricks Database
This project demonstrates the use of Python to interact with a Databricks SQL Server. Specifically, it gives the capability to take the bad-drivers csv and upload relevant data to a table hosted on Databricks called jm_baddrivers. It then creates another table, jm_baddrivers_speed, which was derived from the original table. Finally, it outputs insights generated from a complex query joining and aggregating the two tables that were created. 

In this project template, I get a continuous feedback loop as I iterate and enhance this analysis. I also provide interesting insights regarding the states statistics regarding drivers involved in fatal collisions, or bad drivers. Specifically, I use the average number of bad drivers per billion miles and speeding rates amonst these incidents to analyze the average number drivers that were speeding while involved in a fatal collision. I leverage the Databricks connection, ETL technicques, SQL CRUD statements and complex query elements. 

## CI/CD Badge
[![CICD](https://github.com/nogibjj/JahnaviM-ComplexSQL/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/JahnaviM-ComplexSQL/actions/workflows/cicd.yml)

## Instructions for Use
To use this repository, start by cloning it, or open a codespace. Make sure all the requirements in requirements.txt are fulfilled as these are necessary tools to run the python scripts. In order to connect to Databricks, you will need your own credentials. Create a .env file with your Databricks' secrets. Finally, run the script, etl.py, and review the results from the aggregation.

## Tables + Schema
1. Table Name: jm_baddrivers
| Column Name            | Data Type | Column Description
| ---------------------- | --------- | ------------------------------------------------------------------------------------------------------ |
| state                  | string    | State in the United States of America                                                                  |
| drivers_count          | float     | Number of drivers involved in fatal collisions per billion miles                                       |
| speeding_percent       | float     | Percentage Of Drivers Involved In Fatal Collisions Who Were Speeding                                   |
| alc_percent            | float     | Percentage Of Drivers Involved In Fatal Collisions Who Were Alcohol-Impaired                           |
| no_distraction_percent | float     | Percentage Of Drivers Involved In Fatal Collisions Who Were Not Distracted                             |
| no_prev_percent        | float     | Percentage Of Drivers Involved In Fatal Collisions Who Had Not Been Involved In Any Previous Accidents |
| car_insurance          | float     | Car Insurance Premiums ($)                                                                             |
| insurance_losses       | float     | Losses incurred by insurance companies for collisions per insured driver ($)                           |

2. Table Name: jm_baddrivers_speed
| Column Name | Data Type | Column Description                                                                     |
| ----------- | --------- | -------------------------------------------------------------------------------------- |
| state       | string    | State in the United States of America                                                  |
| speed_ct    | double    | Number of drivers who were speeding and involved in fatal collisions per billion miles |

## Queries + Explanation
1. CTAS for Table 2, jm_baddrivers_speed
```sql
CREATE TABLE jm_baddrivers_speed AS
  SELECT state, drivers_count * speeding_percent/100 as speed_ct
  FROM jm_baddrivers;
```
This statement creates the table jm_baddrivers_speed from the jm_baddrivers table. Using calculations based on existing rows, it creates a column speed_ct. To find the number of drivers who were speeding and involved in fatal collisions per billion miles, it multiplies the percent of bad drivers who were speeding with the number of bad drivers per billion miles.

2. Complex Query with join, aggregation and sorting implemented

```sql
SELECT round(df.drivers_count) as rounded_driv_ct,
  COUNT(df.state) as num_states,
  AVG(df_sp.speed_ct) as avg_ct_speed
FROM jm_baddrivers df
LEFT JOIN jm_baddrivers_speed df_sp ON df.state = df_sp.state
GROUP BY round(df.drivers_count)
ORDER BY round(df.drivers_count);
```
JOIN: The left join on state between the two tables results in a new table with all 51 states and all the bad driver metrics from jm_baddrivers and jm_baddrivers_speed. Because the primary key in the dataset is state, and both datasets share the same set of states, though this join is specifically a left join, all the data between both tables is preserved.

GROUP BY: I used a transformed metric for my group by so that I could get insights based on the number of bad drivers per billion miles. ```round(df.drivers_count)``` rounds the bad driver rate to the nearest whole number.

AGGREGATIONS: (1) ```COUNT(df.state)``` represents the number of states with the approximately the number of bad drivers per billion miles according to the ```rounded_driv_ct``` value.
              (2) ```AVG(df_sp.speed_ct)``` represents the average number of bad drivers per billion miles who speed amongst the states with the total bad drivers per billion miles according to the ```rounded_driv_ct``` 
              value.

ORDER BY: The final output is sorted in ascending order based on the counts in the column ```rounded_driv_ct```


## Example Script Result
<img width="744" alt="image" src="https://github.com/user-attachments/assets/c5cdb2b8-6b14-4d46-8858-c8ccfe9eacec">

Example interpretation of the row where ```rounded_driv_ct```=11.0:
There are 5 states whose number of drivers involved in fatal collisions per billion miles is in the range [10.5, 11.5). On average, these states find approximately 3.97 bad drivers per billion who were also speeding during the collision.

Using this information, one could compare the trend of fatal collisions and speeding rates. 



