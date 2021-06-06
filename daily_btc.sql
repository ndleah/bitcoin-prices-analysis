--inspect the data table
SELECT * FROM trading.daily_btc
LIMIT 5;

/*Result:
|market_date             |open_price  |high_price  |low_price   |close_price |adjusted_close_price|volume      |
|------------------------|------------|------------|------------|------------|--------------------|------------|
|2014-09-17T00:00:00.000Z|465.864014  |468.174011  |452.421997  |457.334015  |457.334015          |21056800    |
|2014-09-18T00:00:00.000Z|456.859985  |456.859985  |413.104004  |424.440002  |424.440002          |34483200    |
|2014-09-19T00:00:00.000Z|424.102997  |427.834991  |384.532013  |394.795990  |394.795990          |37919700    |
|2014-09-20T00:00:00.000Z|394.673004  |423.295990  |389.882996  |408.903992  |408.903992          |36863600    |
|2014-09-21T00:00:00.000Z|408.084991  |412.425995  |393.181000  |398.821014  |398.821014          |26580100    |
*/

--1. Data Exploration
--a. Identify Null Rows
SELECT *
FROM trading.daily_btc
WHERE (
  open_price + high_price + low_price + 
  close_price + adjusted_close_price + volume 
) IS NULL;

--OR

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

/* Result:
|market_date             |open_price  |high_price  |low_price   |close_price |adjusted_close_price|volume      |
|------------------------|------------|------------|------------|------------|--------------------|------------|
|2020-04-17T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
|2020-10-09T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
|2020-10-12T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
|2020-10-13T00:00:00.000Z|null        |null        |null        |null        |null                |null        |
*/

--b. Filling & Update Null Values
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

/*Result:
|market_date             |open_price  |lag_open_price|coalesce_open_price|
|------------------------|------------|--------------|-------------------|
|2020-04-16T00:00:00.000Z|6640.454102 |null          |6640.454102        |
|2020-04-17T00:00:00.000Z|null        |6640.454102   |6640.454102        |
|2020-04-18T00:00:00.000Z|7092.291504 |null          |7092.291504        |
*/

--c. Update Tables
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

/*Result:
|market_date             |open_price  |high_price  |low_price   |close_price |adjusted_close_price|volume     |
|------------------------|------------|------------|------------|------------|--------------------|-----------|
|2020-04-17T00:00:00.000Z|6640.454102 |7134.450684 |6555.504395 |7116.804199 |7116.804199         |46783242377|
|2020-10-09T00:00:00.000Z|10677.625000|10939.799805|10569.823242|10923.627930|10923.627930        |21962121001|
|2020-10-12T00:00:00.000Z|11296.082031|11428.813477|11288.627930|11384.181641|11384.181641        |19968627060|
|2020-10-13T00:00:00.000Z|11296.082031|11428.813477|11288.627930|11384.181641|11384.181641        |19968627060|
*/

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



--2. Analysis
/*a.
****************************************** 
Q1: What is the average daily volume of Bitcoin 
for the last 7 days?
+ 
Q2: Create a 1/0 flag if a specific day is higher
than the last 7 days volume average.
*********************************************/
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

/*Result:
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
*/


/*b.
****************************************** 
Q3: What is the percentage of weeks (starting
on a Monday) where there are 4 or more days with
increased volume?
+ 
Q4: CHow many high volume weeks are there broken
down by year for the weeks with 5-7 days above 
the 7 day volume average excluding 2021?
*********************************************/
--break down by week
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
    2) AS percentage_of_weeks
FROM aggregated_weeks
GROUP BY weekly_high_volume_days
ORDER BY weekly_high_volume_days;

/* Result:
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
*/

--breakdown by year
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

/*Result:
|market_year             |high_volume_weeks|percentage_of_total|
|------------------------|-----------------|-------------------|
|2014                    |2                |2.99               |
|2015                    |3                |4.48               |
|2016                    |13               |19.40              |
|2017                    |17               |25.37              |
|2018                    |8                |11.94              |
|2019                    |11               |16.42              |
|2020                    |13               |19.40              |
*/