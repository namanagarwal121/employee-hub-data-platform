CREATE PROC gold.sp_DimensionTableCreation
AS
BEGIN

TRUNCATE TABLE gold.DimDepartment;
TRUNCATE TABLE gold.DimEmployee;
TRUNCATE TABLE gold.DimJobTitle;

DECLARE @max_id INT = ISNULL((SELECT MAX(DepartmentKey) FROM gold.DimDepartment), 0);
INSERT INTO gold.DimDepartment
(
    DepartmentKey
    ,DepartmentID
    ,DepartmentName
)
SELECT
    ROW_NUMBER() OVER (ORDER BY [Name]) + @max_id,
    CAST([id] AS UNIQUEIDENTIFIER),
    [Name]
FROM silver.departments;

DECLARE @max_id_title INT = ISNULL((SELECT MAX(JobTitleKey) FROM gold.DimJobTitle), 0);
INSERT INTO gold.DimJobTitle
(
    JobTitleKey,
    JobTitleID,
    JobTitle
)
SELECT
    ROW_NUMBER() OVER (ORDER BY Title) + @max_id_title AS JobTitleKey,
    CAST(id AS UNIQUEIDENTIFIER) AS JobTitleID,
    Title AS JobTitle
FROM silver.jobtitles;

DECLARE @max_id_emp INT = ISNULL((SELECT MAX(EmployeeKey) FROM gold.DimEmployee), 0);
INSERT INTO gold.DimEmployee
(
    EmployeeKey,
    EmployeeID,
    EmployeeSourceID,
    FullName,
    Email,
    JoiningDate,
    DepartmentKey,
    JobTitleKey
)
SELECT
    ROW_NUMBER() over (ORDER by [EmployeeID]) + @max_id_emp
    ,e.[EmployeeID]
    ,e.id
    ,e.[FullName]
    ,e.[Email]
    ,e.[JoiningDate]
    ,d.DepartmentKey
    ,j.JobTitleKey
from silver.employees e
JOIN gold.DimDepartment d
ON e.[dept_id] = d.DepartmentID
left JOIN gold.DimJobTitle j 
ON j.JobTitleID = e.[title_id];

END

GO