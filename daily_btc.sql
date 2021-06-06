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
   

DROP TABLE IF EXISTS frame_example;
CREATE TEMP TABLE frame_example AS
WITH input_data (val) AS (
 VALUES
 (1),
 (1),
 (2),
 (6),
 (9),
 (9),
 (20),
 (20),
 (25)
)
SELECT
  val,
  ROW_NUMBER() OVER w AS _row_number,
  DENSE_RANK() OVER w AS _dense_rank
FROM input_data
WINDOW
  w AS (ORDER BY val);

-- inspect the dataset
SELECT * FROM frame_example;