-- ----------------------------------------------------------------------------------------
-- Title: rdz_new_hires_qtr.sql
-- Date: 2023-07-19
-- Author: Friscian Viales
-- Language: Standard SQL - BigQuery
-- ----------------------------------------------------------------------------------------


{{
  config(
    dataset="rdz",
    cluster_by=["department", "job"],
    tags=["rdz"]
  )
}}


WITH

BaseTable AS (
    SELECT DISTINCT
        a.id AS employee_id,
        a.name,
        PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%SZ', a.datetime) AS hire_date,
        b.department,
        c.job
    FROM {{ ref("sdz_hr_hired_employees") }} a
    LEFT JOIN {{ ref("sdz_hr_departments") }} b ON a.department_id = b.id
    LEFT JOIN {{ ref("sdz_hr_jobs") }} c ON a.job_id = c.id
    WHERE b.department IS NOT NULL
),

AggregatedTable AS (
    SELECT
        department,
        job,
        EXTRACT(YEAR FROM hire_date) AS year,
        EXTRACT(QUARTER FROM hire_date) AS quarter,
        COUNT(employee_id) AS new_hire_count
    FROM BaseTable
    WHERE EXTRACT(YEAR FROM hire_date) = 2021
    GROUP BY department, job, year, quarter
),

ResultsTable AS (
    SELECT
        department, job,
        SUM(IF(quarter = 1, new_hire_count, 0)) AS Q1,
        SUM(IF(quarter = 2, new_hire_count, 0)) AS Q2,
        SUM(IF(quarter = 3, new_hire_count, 0)) AS Q3,
        SUM(IF(quarter = 4, new_hire_count, 0)) AS Q4
    FROM AggregatedTable
    GROUP BY department, job
    ORDER BY department, job
)

SELECT * FROM ResultsTable
