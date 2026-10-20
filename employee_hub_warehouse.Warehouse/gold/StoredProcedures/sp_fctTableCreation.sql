CREATE PROC gold.sp_fctTableCreation
AS
BEGIN

TRUNCATE TABLE gold.FactEmployeeSnapshot;

INSERT INTO gold.FactEmployeeSnapshot
(
    SnapshotDate,
    EmployeeKey,
    DepartmentKey,
    JobTitleKey,
    Salary,
    EmployeeCount
)
SELECT
    CAST(GETDATE() AS DATE) AS SnapshotDate,
    e.EmployeeKey,
    d.DepartmentKey,
    j.JobTitleKey,
    s.Salary,
    1 AS EmployeeCount
FROM silver.employees s
INNER JOIN gold.DimEmployee e
    ON s.EmployeeID = e.EmployeeID
INNER JOIN gold.DimDepartment d
    ON TRY_CAST(s.dept_id AS UNIQUEIDENTIFIER) = d.DepartmentID
INNER JOIN gold.DimJobTitle j
    ON TRY_CAST(s.title_id AS UNIQUEIDENTIFIER) = j.JobTitleID;

END

GO