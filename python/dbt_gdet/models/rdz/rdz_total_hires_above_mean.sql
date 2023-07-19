-- ----------------------------------------------------------------------------------------
-- Title: rdz_total_hires_above_mean.sql
-- Date: 2023-07-19
-- Author: Friscian Viales
-- Language: Standard SQL - BigQuery
-- ----------------------------------------------------------------------------------------


{{
  config(
    dataset="rdz",
    cluster_by=["department"],
    tags=["rdz"]
  )
}}


WITH

BaseTable AS (
    SELECT DISTINCT
        a.id AS hire_id,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', a.datetime) AS hire_date,
        b.id AS dept_id,
        b.department,
    FROM {{ source("sdz", "sdz_hr_hired_employees") }} a
    LEFT JOIN {{ source("sdz", "sdz_hr_departments") }} b ON a.department_id = b.id
    WHERE b.department IS NOT NULL
),

Average2021 AS (
  SELECT
    dept_id,
    COUNT(hire_id) AS mean_hires
  FROM BaseTable
  WHERE EXTRACT(YEAR FROM hire_date) = 2021
  GROUP BY 1
),

AggregatedTable AS (
  SELECT
    dept_id, department,
    COUNT(hire_id) total_hires
  FROM BaseTable
  GROUP BY 1, 2
),

ResultTable AS (
  SELECT
    a.*, b.mean_hires
  FROM AggregatedTable a
  LEFT JOIN Average2021 b ON a.dept_id = b.dept_id
  WHERE a.total_hires > b.mean_hires
  ORDER BY 3 DESC
)

SELECT * FROM ResultTable
