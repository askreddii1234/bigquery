To perform data profiling for an entire BigQuery table, you can use SQL queries that generate a complete data summary, including:

Column names & data types
Missing values & unique values
Min, Max, Mean, Median, Standard Deviation
Outlier detection
Anomaly detection using ML (optional)
1. Full Table Data Profiling SQL Query
This SQL query provides a complete profile of each column in a BigQuery table.

sql
Copy
Edit
WITH column_info AS (
  SELECT 
    column_name,
    data_type
  FROM `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'your_table'
),
column_stats AS (
  SELECT 
    column_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT column_name) AS unique_values,
    SUM(CASE WHEN column_name IS NULL THEN 1 ELSE 0 END) AS missing_values,
    MIN(column_name) AS min_value,
    MAX(column_name) AS max_value,
    AVG(SAFE_CAST(column_name AS FLOAT64)) AS avg_value,
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 100)[OFFSET(50)] AS median_value,
    STDDEV(SAFE_CAST(column_name AS FLOAT64)) AS stddev_value
  FROM `your_project.your_dataset.your_table`
  GROUP BY column_name
),
outlier_detection AS (
  SELECT 
    column_name,
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 4) AS quartiles
  FROM `your_project.your_dataset.your_table`
)
SELECT 
  ci.column_name,
  ci.data_type,
  cs.total_records,
  cs.unique_values,
  cs.missing_values,
  cs.min_value,
  cs.max_value,
  cs.avg_value,
  cs.median_value,
  cs.stddev_value,
  od.quartiles[OFFSET(1)] AS Q1,  
  od.quartiles[OFFSET(2)] AS Q2,  
  od.quartiles[OFFSET(3)] AS Q3   
FROM column_info ci
LEFT JOIN column_stats cs ON ci.column_name = cs.column_name
LEFT JOIN outlier_detection od ON ci.column_name = od.column_name;
2. Automate Data Profiling with a Stored Procedure
You can automate the data profiling using a stored procedure in BigQuery.

sql
Copy
Edit
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.data_profiling`()
BEGIN
  -- Column Summary
  SELECT 
    column_name, 
    data_type, 
    COUNT(*) AS total_records, 
    COUNT(DISTINCT column_name) AS unique_values,
    SUM(CASE WHEN column_name IS NULL THEN 1 ELSE 0 END) AS missing_values,
    MIN(column_name) AS min_value,
    MAX(column_name) AS max_value,
    AVG(SAFE_CAST(column_name AS FLOAT64)) AS avg_value,
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 100)[OFFSET(50)] AS median_value,
    STDDEV(SAFE_CAST(column_name AS FLOAT64)) AS stddev_value
  FROM `your_project.your_dataset.your_table`
  JOIN `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
  ON table_name = 'your_table'
  GROUP BY column_name, data_type;

  -- Outlier Detection
  SELECT 
    column_name, 
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 4) AS quartiles
  FROM `your_project.your_dataset.your_table`;

  -- Anomaly Detection using K-Means Clustering
  CREATE OR REPLACE MODEL `your_project.your_dataset.kmeans_model`
  OPTIONS(model_type='kmeans', num_clusters=3) AS
  SELECT column_name FROM `your_project.your_dataset.your_table`;

  SELECT 
    column_name, 
    cluster_label, 
    distance
  FROM ML.PREDICT(MODEL `your_project.your_dataset.kmeans_model`, 
                  (SELECT column_name FROM `your_project.your_dataset.your_table`))
  ORDER BY distance DESC
  LIMIT 10;
END;
Now, you can run the stored procedure:

sql
Copy
Edit
CALL `your_project.your_dataset.data_profiling`();
3. Use bq CLI for Automated Profiling
If you want to export profiling results for further analysis, use the BigQuery CLI:

bash
Copy
Edit
bq query --use_legacy_sql=false 'SELECT * FROM `your_project.your_dataset.your_table` LIMIT 10'
Or save results to a new table:

sql
Copy
Edit
CREATE OR REPLACE TABLE `your_project.your_dataset.profiling_results` AS 
SELECT * FROM `your_project.your_dataset.your_table` LIMIT 100;
Key Insights Provided
✔ Data Types
✔ Null & Missing Values
✔ Min, Max, Mean, Median, Stddev
✔ Outlier Detection (Using Quartiles)
✔ Anomaly Detection (Using K-Means ML)
✔ Automated Profiling via Stored Procedure

Would you like any additional custom metrics (like correlation or data skewness)? 🚀







You said:
Yes please 
ChatGPT said:
Here is an enhanced BigQuery data profiling script that includes correlation analysis, data skewness detection, and additional insights.

1. Full Table Data Profiling with Correlation & Skewness
This query will:

Summarize the dataset (nulls, unique values, min, max, mean, median, stddev)
Detect outliers using quartiles (IQR method)
Compute correlation matrix for numeric columns
Check data skewness (log transformation test)
sql
Copy
Edit
WITH column_info AS (
  SELECT 
    column_name,
    data_type
  FROM `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'your_table'
),
column_stats AS (
  SELECT 
    column_name,
    COUNT(*) AS total_records,
    COUNT(DISTINCT column_name) AS unique_values,
    SUM(CASE WHEN column_name IS NULL THEN 1 ELSE 0 END) AS missing_values,
    MIN(SAFE_CAST(column_name AS FLOAT64)) AS min_value,
    MAX(SAFE_CAST(column_name AS FLOAT64)) AS max_value,
    AVG(SAFE_CAST(column_name AS FLOAT64)) AS avg_value,
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 100)[OFFSET(50)] AS median_value,
    STDDEV(SAFE_CAST(column_name AS FLOAT64)) AS stddev_value
  FROM `your_project.your_dataset.your_table`
  GROUP BY column_name
),
outlier_detection AS (
  SELECT 
    column_name,
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 4) AS quartiles
  FROM `your_project.your_dataset.your_table`
),
correlation_matrix AS (
  SELECT
    CORR(col1, col2) AS correlation_score,
    col1,
    col2
  FROM (
    SELECT 
      SAFE_CAST(column1 AS FLOAT64) AS col1, 
      SAFE_CAST(column2 AS FLOAT64) AS col2
    FROM `your_project.your_dataset.your_table`
  )
  WHERE col1 IS NOT NULL AND col2 IS NOT NULL
),
skewness_check AS (
  SELECT 
    column_name,
    COUNT(*) AS total_records,
    SAFE_CAST(LOG(SAFE_CAST(column_name AS FLOAT64) + 1) AS FLOAT64) AS log_transformed_value
  FROM `your_project.your_dataset.your_table`
)
SELECT 
  ci.column_name,
  ci.data_type,
  cs.total_records,
  cs.unique_values,
  cs.missing_values,
  cs.min_value,
  cs.max_value,
  cs.avg_value,
  cs.median_value,
  cs.stddev_value,
  od.quartiles[OFFSET(1)] AS Q1,  
  od.quartiles[OFFSET(2)] AS Q2,  
  od.quartiles[OFFSET(3)] AS Q3,
  corr.correlation_score,
  skew.log_transformed_value
FROM column_info ci
LEFT JOIN column_stats cs ON ci.column_name = cs.column_name
LEFT JOIN outlier_detection od ON ci.column_name = od.column_name
LEFT JOIN correlation_matrix corr ON ci.column_name = corr.col1
LEFT JOIN skewness_check skew ON ci.column_name = skew.column_name;
2. Automate Data Profiling with a Stored Procedure
This stored procedure automatically profiles every numeric and categorical column in the table.

sql
Copy
Edit
CREATE OR REPLACE PROCEDURE `your_project.your_dataset.data_profiling`()
BEGIN
  -- Column Summary
  SELECT 
    column_name, 
    data_type, 
    COUNT(*) AS total_records, 
    COUNT(DISTINCT column_name) AS unique_values,
    SUM(CASE WHEN column_name IS NULL THEN 1 ELSE 0 END) AS missing_values,
    MIN(SAFE_CAST(column_name AS FLOAT64)) AS min_value,
    MAX(SAFE_CAST(column_name AS FLOAT64)) AS max_value,
    AVG(SAFE_CAST(column_name AS FLOAT64)) AS avg_value,
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 100)[OFFSET(50)] AS median_value,
    STDDEV(SAFE_CAST(column_name AS FLOAT64)) AS stddev_value
  FROM `your_project.your_dataset.your_table`
  JOIN `your_project.your_dataset.INFORMATION_SCHEMA.COLUMNS`
  ON table_name = 'your_table'
  GROUP BY column_name, data_type;

  -- Outlier Detection
  SELECT 
    column_name, 
    APPROX_QUANTILES(SAFE_CAST(column_name AS FLOAT64), 4) AS quartiles
  FROM `your_project.your_dataset.your_table`;

  -- Correlation Analysis
  SELECT 
    col1, 
    col2, 
    CORR(col1, col2) AS correlation_score
  FROM (
    SELECT 
      SAFE_CAST(column1 AS FLOAT64) AS col1, 
      SAFE_CAST(column2 AS FLOAT64) AS col2
    FROM `your_project.your_dataset.your_table`
  )
  WHERE col1 IS NOT NULL AND col2 IS NOT NULL;

  -- Skewness Check (Log Transformation)
  SELECT 
    column_name,
    COUNT(*) AS total_records,
    SAFE_CAST(LOG(SAFE_CAST(column_name AS FLOAT64) + 1) AS FLOAT64) AS log_transformed_value
  FROM `your_project.your_dataset.your_table`;
END;
Now, simply run the procedure:

sql
Copy
Edit
CALL `your_project.your_dataset.data_profiling`();
3. Additional Metrics Explained
✅ Correlation Analysis
Uses CORR() function to compute relationships between numeric columns.
Helps detect highly correlated features (useful for feature selection in ML).
✅ Skewness Detection
Uses log transformation to check for skewed distributions.
If log-transformed data follows a normal distribution, the column was skewed before.
✅ Quartiles for Outlier Detection
If a value is far above Q3 or below Q1, it's an outlier.
Helps remove erroneous data points before ML training.
Which Insights Are Available?
✔ Data Types
✔ Missing Values & Unique Counts
✔ Min, Max, Mean, Median, Standard Deviation
✔ Outlier Detection (Quartiles/IQR Method)
✔ Correlation Matrix
✔ Data Skewness Analysis

