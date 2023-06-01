WITH base_query AS (
	SELECT	
		emp.id AS empId, 
		emp.ename AS name, 
		emp.datetime AS datetime, 
		jobs.id AS jobId, 
		jobs.job AS job, 
		dep.id AS depId, 
		dep.department AS department 
	FROM dbo.hired_employees emp
	inner join dbo.jobs jobs
		ON emp.job_id = jobs.id
	inner join dbo.departments dep
		ON emp.department_id = dep.id
	WHERE year(emp.datetime) = 2021
),

ALLQ AS(
	SELECT	
		job, 
		department,	
		DATEPART(QUARTER, datetime) as Q, 
		COUNT(*) AS amount 
	FROM base_query
	GROUP BY job, department, DATEPART(QUARTER, datetime)
),

quarters AS(
	SELECT 
		department, 
		job, 
		COALESCE([1], 0) Q1,
		COALESCE([2], 0) Q2,
		COALESCE([3], 0) Q3,
		COALESCE([4], 0) Q4 
	FROM ALLQ
	PIVOT(MAX(amount)
	FOR Q IN ([1],[2],[3],[4])) q
),

MEDIAN AS(
	SELECT AVG(TOTAL) MEDIA 
	FROM(
		SELECT SUM(AMOUNT) TOTAL from ALLQ
		GROUP BY department
	) FINAL
),

TOTAL_DEP AS(
	select 
		depId, 
		department, 
		COUNT(*) TOTAL_DEP from 
	base_query
	GROUP BY depId, department
),

MAY_DEP AS(
	SELECT TOT_DEP.* FROM (
		SELECT * FROM TOTAL_DEP
	)TOT_DEP,
	(
		SELECT * FROM MEDIAN
	)MED
	WHERE TOT_DEP.TOTAL_DEP > MED.MEDIA
)

SELECT * FROM MAY_DEP
ORDER BY TOTAL_DEP DESC;



