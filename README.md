# Bitcoin-Trading-Volume-And-Prices
 [![View Repositories](https://img.shields.io/badge/View-My_Repositories-blue?logo=GitHub)](https://github.com/nduongthucanh?tab=repositories)
[![View My Profile](https://img.shields.io/badge/View-My_Profile-green?logo=GitHub)](https://github.com/nduongthucanh) 
![fork](https://img.shields.io/github/forks/nduongthucanh/Health-Analytics-Mini-Case-Study) ![star](https://img.shields.io/github/stars/nduongthucanh/Health-Analytics-Mini-Case-Study)



# BITCOIN TRADING VOLUME AND PRICES ANALYSIS


<p align="center">
  <img src="https://forthebadge.com/images/badges/built-with-love.svg">
  <img src="https://forthebadge.com/images/badges/powered-by-coffee.svg">
</p>

<p align="center">
  <img src="https://forthebadge.com/images/badges/check-it-out.svg">
</p>

<p align="center">
  <img width="100%" height="100%" src="https://github.com/nduongthucanh/Bitcoin-Trading-Volume-And-Prices/blob/main/IMG/bitcoin-cover.gif">
</p>

## Table of contents
<!--ts-->
   * [Overview](#OVERVIEW)
   * [Solutions](#SOLUTIONS)
     * [Data Exploration](#data-exploration)
     * [Weekly Volume Comparison Analysis](#weekly-volume-comparison-analysis)
       * [Average Volume](#average-volumes)
       * [High Volume Weeks](#high-volume-weeks)
     * [Statistical Analysis](#statistical-analysis)
       * [Simple Moving Average](#simple-moving-average)
       * [Weighted Moving Average](#weighted-moving-average)
       * [EWMA](#exponential-weighted-moving-average-ewma)


---
  
## OVERVIEW
This case study is contained within the [Serious SQL](https://www.datawithdanny.com) by [Danny Ma](https://www.linkedin.com/in/datawithdanny/). With this case study, I covered almost several practices of window functions, including:

### Components of Window Functions
* **```PARTITION BY```** Clause and Multiple Aggregations
* Calculating over an entire dataset using the empty **```OVER```** 
* Ordered analytical window functions (clause **```FROM```**, **```WHERE```** filters, **```ON```** table join conditions, **```GROUP BY```**, **```SELECT```** aggregate function calculations,  **```HAVING```**, **```ORDER BY```**, **```LIMIT```**)

### Advanced Window Functions
* Proper usage of **```LEAD```** and **```LAG```** functions with a view on data philosophy
* Null propagation to identify rows with NULL values 
* Using **```COALESCE```** to replace missing values 
* Using the **```OFFSET```** and **```LAG/LEAD```** functions to skip rows
* Basic cumulative sums
* Using a WINDOW clause at the end of a query to reduce code repetition
* Window frame components
---
## SOLUTIONS
### Data Exploration
1. **Identify Null Rows**
```sql 
SELECT *
FROM trading.daily_btc
WHERE (
  open_price + high_price + low_price + 
  close_price + adjusted_close_price + volume 
) IS NULL;
```
OR
```sql
SELECT *
FROM trading.daily_btc
WHERE
  market_date IS NULL
  OR open_price IS NULL
  OR high_price IS NULL
  OR low_price IS NULL
  OR close_price IS NULL
  OR adjusted_close_price IS NULL
  OR volume IS NULL;
```
**Result:**
|market_date             |open_price  |high_price  |low_price   |close_price |adjusted_close_price|volume      |
|------------------------|------------|------------|------------|------------|--------------------|------------|
|2020-04-17T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
|2020-10-09T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
|2020-10-12T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
|2020-10-13T00:00:00.000Z|null        |null        |null        |null        |null                |null        |


2. **Filling & Update Null Values**
```sql
WITH april_17_data AS (
  SELECT
  market_date,
  open_price,
  LAG(open_price) OVER (ORDER BY market_date) AS lag_open_price
FROM trading.daily_btc
WHERE market_date BETWEEN ('2020-04-17'::DATE - 1) AND ('2020-04-17'::DATE + 1)
)
SELECT
  market_date,
  open_price,
  lag_open_price,
  COALESCE(open_price, lag_open_price) AS coalesce_open_price
FROM april_17_data;
```
**Result:**
|market_date             |open_price  |lag_open_price|coalesce_open_price|
|------------------------|------------|--------------|-------------------|
|2020-04-16T00:00:00.000Z|6640.454102 |null          |6640.454102        |
|2020-04-17T00:00:00.000Z|null        |6640.454102   |6640.454102        |
|2020-04-18T00:00:00.000Z|7092.291504 |null          |7092.291504        |

3.  **Update Tables**
```sql
DROP TABLE IF EXISTS updated_daily_btc;
CREATE TEMP TABLE updated_daily_btc AS
SELECT
  market_date,
  COALESCE(
    open_price,
      LAG(open_price, 1) OVER (ORDER BY market_date),
      LAG(open_price, 2) OVER (ORDER BY market_date)
  ) AS open_price,
  COALESCE(
    high_price,
      LAG(high_price, 1) OVER (ORDER BY market_date),
      LAG(high_price, 2) OVER (ORDER BY market_date)
  ) AS high_price,
  COALESCE(
    low_price,
      LAG(low_price, 1) OVER (ORDER BY market_date),
      LAG(low_price, 2) OVER (ORDER BY market_date)
  ) AS low_price,
  COALESCE(
    close_price,
      LAG(close_price, 1) OVER (ORDER BY market_date),
      LAG(close_price, 2) OVER (ORDER BY market_date)
  ) AS close_price,
  COALESCE(
    adjusted_close_price,
      LAG(adjusted_close_price, 1) OVER (ORDER BY market_date),
      LAG(adjusted_close_price, 2) OVER (ORDER BY market_date)
  ) AS adjusted_close_price,
   COALESCE(
    volume,
      LAG(volume, 1) OVER (ORDER BY market_date),
      LAG(volume, 2) OVER (ORDER BY market_date)
  ) AS volume
FROM trading.daily_btc;

SELECT *
FROM updated_daily_btc
WHERE market_date IN (
  '2020-04-17',
  '2020-10-09',
  '2020-10-12',
  '2020-10-13'
);
```
**Result:**
|market_date             |open_price  |high_price  |low_price   |close_price |adjusted_close_price|volume     |
|------------------------|------------|------------|------------|------------|--------------------|-----------|
|2020-04-17T00:00:00.000Z|6640.454102 |7134.450684 |6555.504395 |7116.804199 |7116.804199         |46783242377|
|2020-10-09T00:00:00.000Z|10677.625000|10939.799805|10569.823242|10923.627930|10923.627930        |21962121001|
|2020-10-12T00:00:00.000Z|11296.082031|11428.813477|11288.627930|11384.181641|11384.181641        |19968627060|
|2020-10-13T00:00:00.000Z|11296.082031|11428.813477|11288.627930|11384.181641|11384.181641        |19968627060|
---
### Weekly Volume Comparison Analysis
The following are a set of questions that i would use to practice using SQL window functions as well as discover insights in this case study:

1. What is the average daily volume of Bitcoin for the last 7 days?
2. Create a 1/0 flag if a specific day is higher than the last 7 days volume average
3. What is the percentage of weeks (starting on a Monday) where there are 4 or more days with increased volume?
4. How many high volume weeks are there broken down by year for the weeks with 5-7 days above the 7 day volume average excluding 2021?

#### **Average Volumes**
> This section contains solution for Question 1 and 2
```sql
WITH window_calculations AS (
SELECT
  market_date,
  volume,
  ROUND(
    AVG(volume) OVER (
    ORDER BY market_date
    RANGE BETWEEN '7 DAYS' PRECEDING AND '1 DAY' PRECEDING)
  ) AS past_weekly_avg_volume
FROM updated_daily_btc
)
SELECT
  market_date,
  volume,
  past_weekly_avg_volume,
  CASE 
    WHEN volume > past_weekly_avg_volume THEN 1
    ELSE 0
    END AS volume_flag
FROM window_calculations
ORDER BY market_date DESC
LIMIT 10;
```

**Result:**
|market_date             |volume      |past_weekly_avg_volume|volume_flag|
|------------------------|------------|----------------------|-----------|
|2021-02-24T00:00:00.000Z|88364793856 |73509817753           |1          |
|2021-02-23T00:00:00.000Z|106102492824|69359402048           |1          |
|2021-02-22T00:00:00.000Z|92052420332 |67219042453           |1          |
|2021-02-21T00:00:00.000Z|51897585191 |69983483887           |0          |
|2021-02-20T00:00:00.000Z|68145460026 |70284197619           |0          |
|2021-02-19T00:00:00.000Z|63495496918 |72149846802           |0          |
|2021-02-18T00:00:00.000Z|52054723579 |76340445121           |0          |
|2021-02-17T00:00:00.000Z|80820545404 |77266237191           |1          |
|2021-02-16T00:00:00.000Z|77049582886 |79374846334           |0          |
|2021-02-15T00:00:00.000Z|77069903166 |82860177694           |0          |

#### **High Volume Weeks**
> This section contains solution for Question 3 and 4
1. **Break down by week**
```sql
WITH window_calculations AS (
SELECT
  market_date,
  volume,
  ROUND(
    AVG(volume) OVER (
    ORDER BY market_date
    RANGE BETWEEN '7 DAYS' PRECEDING AND '1 DAY' PRECEDING)
  ) AS past_weekly_avg_volume
FROM updated_daily_btc
),
--generate the date
date_calculations AS (
SELECT
  market_date,
  DATE_TRUNC('week', market_date)::DATE start_of_week,
  volume,
  CASE
    WHEN volume > past_weekly_avg_volume THEN 1
    ELSE 0
    END AS volume_flag
FROM window_calculations
),
--aggregate the metrics
aggregated_weeks AS (
SELECT
  start_of_week,
  SUM(volume_flag) AS weekly_high_volume_days
  FROM date_calculations
  GROUP BY start_of_week
)
--calculate the percentage
SELECT
  weekly_high_volume_days,
  ROUND (
    100 * COUNT(*)/SUM(COUNT(*)) OVER (), 
    1) AS percentage_of_weeks
FROM aggregated_weeks
GROUP BY weekly_high_volume_days
ORDER BY weekly_high_volume_days;
```

**Result:**
|weekly_high_volume_days |percentage_of_weeks|
|------------------------|-------------------|
|0                       |6.23               |
|1                       |13.65              |
|2                       |20.47              |
|3                       |20.47              |
|4                       |18.99              |
|5                       |11.87              |
|6                       |6.23               |
|7                       |2.08               |


1. **Break down by year**
```sql
WITH window_calculations AS (
SELECT
  market_date,
  volume,
  ROUND(
    AVG(volume) OVER (
    ORDER BY market_date
    RANGE BETWEEN '7 DAYS' PRECEDING AND '1 DAY' PRECEDING)
  ) AS past_weekly_avg_volume
FROM updated_daily_btc
),
--generate the date
date_calculations AS (
SELECT
  market_date,
  DATE_TRUNC('week', market_date)::DATE start_of_week,
  volume,
  CASE
    WHEN volume > past_weekly_avg_volume THEN 1
    ELSE 0
    END AS volume_flag
FROM window_calculations
),
--aggregate the metrics
aggregated_weeks AS (
SELECT
  start_of_week,
  SUM(volume_flag) AS weekly_high_volume_days
  FROM date_calculations
  GROUP BY start_of_week
)
--calculate the percentage (by year)
SELECT
  EXTRACT(YEAR FROM start_of_week) AS market_year,
  COUNT(*) AS high_volume_weeks,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM aggregated_weeks
WHERE weekly_high_volume_days >= 5
AND start_of_week < '2021-01-01'::DATE
GROUP BY 1
ORDER BY 1;
```

**Result:**
|market_year             |high_volume_weeks|percentage_of_total|
|------------------------|-----------------|-------------------|
|2014                    |2                |2.99               |
|2015                    |3                |4.48               |
|2016                    |13               |19.40              |
|2017                    |17               |25.37              |
|2018                    |8                |11.94              |
|2019                    |11               |16.42              |
|2020                    |13               |19.40              |
---
### Statistical Analysis
#### **Simple moving average**
For the next exercise - we will be calculating metrics not just a single window function but multiple in the same query.

For the following time windows: **14, 28, 60, 150 days** - calculate the following metrics for the close_price column:

* Moving average
* Moving standard deviation
* The maximum and minimum values

```sql
WITH base_data AS (
  SELECT
    market_date,
    close_price,
    -- averages
    ROUND(AVG(close_price) OVER w_14) AS avg_14,
    ROUND(AVG(close_price) OVER w_28) AS avg_28,
    ROUND(AVG(close_price) OVER w_60) AS avg_60,
    ROUND(AVG(close_price) OVER w_150) AS avg_150,
    -- standard deviation
    ROUND(STDDEV(close_price) OVER w_14) AS std_14,
    ROUND(STDDEV(close_price) OVER w_28) AS std_28,
    ROUND(STDDEV(close_price) OVER w_60) AS std_60,
    ROUND(STDDEV(close_price) OVER w_150) AS std_150,
    -- max
    ROUND(MAX(close_price) OVER w_14) AS max_14,
    ROUND(MAX(close_price) OVER w_28) AS max_28,
    ROUND(MAX(close_price) OVER w_60) AS max_60,
    ROUND(MAX(close_price) OVER w_150) AS max_150,
    -- min
    ROUND(MIN(close_price) OVER w_14) AS min_14,
    ROUND(MIN(close_price) OVER w_28) AS min_28,
    ROUND(MIN(close_price) OVER w_60) AS min_60,
    ROUND(MIN(close_price) OVER w_150) AS min_150
  FROM updated_daily_btc
  WINDOW
    w_14 AS (ORDER BY MARKET_DATE RANGE BETWEEN '14 DAYS' PRECEDING AND '1 DAY' PRECEDING),
    w_28 AS (ORDER BY MARKET_DATE RANGE BETWEEN '28 DAYS' PRECEDING AND '1 DAY' PRECEDING),
    w_60 AS (ORDER BY MARKET_DATE RANGE BETWEEN '60 DAYS' PRECEDING AND '1 DAY' PRECEDING),
    w_150 AS (ORDER BY MARKET_DATE RANGE BETWEEN '150 DAYS' PRECEDING AND '1 DAY' PRECEDING)
)
SELECT
  market_date,
  close_price,
  CASE
    WHEN close_price BETWEEN
      (avg_14 - 2 * std_14) AND (avg_14 + 2 * std_14)
      THEN 0
    ELSE 1
    END AS outlier_14,
  CASE
    WHEN close_price BETWEEN
      (avg_28 - 2 * std_28) AND (avg_28 + 2 * std_28)
      THEN 0
    ELSE 1
    END AS outlier_28,
  CASE
    WHEN close_price BETWEEN
      (avg_60 - 2 * std_60) AND (avg_60 + 2 * std_60)
      THEN 0
    ELSE 1
    END AS outlier_60,
  CASE
    WHEN close_price BETWEEN
      (avg_150 - 2 * std_150) AND (avg_150 + 2 * std_150)
      THEN 0
    ELSE 1
    END AS outlier_150
FROM base_data
ORDER BY market_date DESC
LIMIT 10;
;
```

**Result:**
|market_date             |close_price |outlier_14|outlier_28|outlier_60|outlier_150|
|------------------------|------------|----------|----------|----------|-----------|
|2021-02-24T00:00:00.000Z|50460.234375|0         |0         |0         |1          |
|2021-02-23T00:00:00.000Z|48824.425781|0         |0         |0         |0          |
|2021-02-22T00:00:00.000Z|54207.320313|0         |0         |1         |1          |
|2021-02-21T00:00:00.000Z|57539.945313|1         |0         |1         |1          |
|2021-02-20T00:00:00.000Z|56099.519531|0         |0         |1         |1          |
|2021-02-19T00:00:00.000Z|55888.132813|1         |1         |1         |1          |
|2021-02-18T00:00:00.000Z|51679.796875|0         |0         |1         |1          |
|2021-02-17T00:00:00.000Z|52149.007813|0         |1         |1         |1          |
|2021-02-16T00:00:00.000Z|49199.871094|0         |0         |1         |1          |
|2021-02-15T00:00:00.000Z|47945.058594|0         |0         |1         |1          |

**FINDING:**
There are 1 values for 60 and 150 day outliers for the majority of the rows or is it expected.

**ILLUSTRATION:**
<p align="center">
  <img width="100%" height="100%" src="https://github.com/nduongthucanh/Bitcoin-Trading-Volume-And-Prices/blob/main/IMG/SMA.png">
</p>

#### **Weighted moving average**

In this section, we were tasked by a demanding Cryptocurrency trading client who required custom weights to be applied using the following combination of simple moving averages:
    
|Time Period|Weight Factor|
|---------- |------------ |
|1-14 days  |0.5          |
|15-28 days |0.3          |
|29-60 days |0.15         |
|61-150 days|0.05         |

```sql
WITH base_data AS (
  SELECT
    market_date,
    ROUND(close_price, 2) AS close_price,
    ROUND(AVG(close_price) OVER w_1_to_14) AS avg_1_to_14,
    ROUND(AVG(close_price) OVER w_15_28) AS avg_15_28,
    ROUND(AVG(close_price) OVER w_29_60) AS avg_29_60,
    ROUND(AVG(close_price) OVER w_61_150) AS avg_61_150
FROM updated_daily_btc
WINDOW
  w_1_to_14 AS (ORDER BY MARKET_DATE RANGE BETWEEN
    '14 DAYS' PRECEDING AND '1 DAY' PRECEDING),
  w_15_28 AS (ORDER BY MARKET_DATE RANGE BETWEEN
    '28 DAYS' PRECEDING AND '15 DAY' PRECEDING),
  w_29_60 AS (ORDER BY MARKET_DATE RANGE BETWEEN
    '60 DAYS' PRECEDING AND '29 DAY' PRECEDING),
  w_61_150 AS (ORDER BY MARKET_DATE RANGE BETWEEN
  '150 DAYS' PRECEDING AND '61 DAY' PRECEDING)
)
SELECT
  market_date,
  close_price,
  0.5 * avg_1_to_14 + 0.3 * avg_15_28 + 0.15 * avg_29_60 + 0.05 * avg_61_150 AS custom_moving_avg
FROM base_data
ORDER BY market_date DESC
LIMIT 10;
```

**Result:**
|market_date             |close_price |custom_moving_avg|
|------------------------|------------|-----------------|
|2021-02-24T00:00:00.000Z|50460.23    |42249.35         |
|2021-02-23T00:00:00.000Z|48824.43    |41822.85         |
|2021-02-22T00:00:00.000Z|54207.32    |41192.25         |
|2021-02-21T00:00:00.000Z|57539.95    |40335.75         |
|2021-02-20T00:00:00.000Z|56099.52    |39534.15         |
|2021-02-19T00:00:00.000Z|55888.13    |38735.40         |
|2021-02-18T00:00:00.000Z|51679.80    |38036.50         |
|2021-02-17T00:00:00.000Z|52149.01    |37408.70         |
|2021-02-16T00:00:00.000Z|49199.87    |36864.40         |
|2021-02-15T00:00:00.000Z|47945.06    |36344.80         |

#### **Exponential Weighted Moving Average (EWMA)**
```sql
DROP TABLE IF EXISTS base_table;
CREATE TEMP TABLE base_table AS
SELECT
    market_date,
    ROUND(close_price, 2) AS close_price,
    ROUND(AVG(close_price) OVER w_1_to_14) AS sma_14,
    ROW_NUMBER() OVER w_1_to_14 AS _row_number
FROM updated_daily_btc
WINDOW
  w_1_to_14 AS (ORDER BY MARKET_DATE RANGE BETWEEN
    '14 DAYS' PRECEDING AND '1 DAY' PRECEDING);

DROP TABLE IF EXISTS ewma_output;
CREATE TEMP TABLE ewma_output AS
WITH RECURSIVE output_table
(market_date, close_price, sma_14, ewma_14, _row_number)
AS (

-- The 1st query: we set initial output row using _row_number = 15
  SELECT
    market_date,
    close_price,
    sma_14,
    sma_14 AS ewma_14,
    _row_number
  FROM base_table
  WHERE _row_number = 15

  UNION ALL

  -- The 2nd query: we need to "shift" our output by 1 row
  SELECT
    base_table.market_date,
    base_table.close_price,
    base_table.sma_14,
    -- Let's round out ewma_14 record to 2 decimal places
    ROUND(
      0.13 * base_table.sma_14 + (1 - 0.13) * output_table.ewma_14,
      2
    ) AS ewma_14,
    base_table._row_number
  FROM output_table
  INNER JOIN base_table
    ON output_table._row_number + 1 = base_table._row_number
    AND base_table._row_number > 15
)
SELECT * FROM output_table;

-- Pivot data for visualisation
SELECT
  market_date,
  'SMA' AS metric_name,
  sma_14 AS metric_value
FROM ewma_output
UNION
SELECT
  market_date,
  'EWMA' AS metric_name,
  ewma_14 AS metric_value
FROM ewma_output;

SELECT
  market_date,
  'Price' AS metric_name,
  close_price AS metric_value
FROM ewma_output
ORDER BY market_date, metric_name;
```  

**ILLUSTRATION:**
<p align="center">
  <img width="100%" height="100%" src="https://github.com/nduongthucanh/Bitcoin-Trading-Volume-And-Prices/blob/main/IMG/EWMA.png">
</p>

___________________________________

<p>&copy; 2021 Leah Nguyen</p>