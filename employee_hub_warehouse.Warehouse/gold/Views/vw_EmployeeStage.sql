-- Current silver employees with their dimension keys resolved. A missing or
-- unknown department / job title maps to the -1 "Unknown" member, so no
-- employee is dropped. RowHash covers the attributes DimEmployee versions on.
CREATE VIEW [gold].[vw_EmployeeStage]
AS
SELECT
    k.EmployeeSourceID,
    k.EmployeeID,
    k.FullName,
    k.Email,
    k.JoiningDate,
    k.DepartmentKey,
    k.JobTitleKey,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT(
        ISNULL(k.FullName, ''), '|',
        ISNULL(k.Email, ''), '|',
        ISNULL(CONVERT(VARCHAR(10), k.JoiningDate, 23), ''), '|',
        k.DepartmentKey, '|',
        k.JobTitleKey)), 2) AS RowHash
FROM (
    SELECT
        CAST(s.[id] AS UNIQUEIDENTIFIER)           AS EmployeeSourceID,
        s.[EmployeeID],
        s.[FullName],
        s.[Email],
        CAST(s.[JoiningDate] AS DATE)              AS JoiningDate,
        ISNULL(d.[DepartmentKey], -1)              AS DepartmentKey,
        ISNULL(j.[JobTitleKey], -1)                AS JobTitleKey
    FROM [silver].[employees] AS s
    LEFT JOIN [gold].[DimDepartment] AS d
        ON d.[DepartmentID] = TRY_CAST(s.[dept_id] AS UNIQUEIDENTIFIER)
    LEFT JOIN [gold].[DimJobTitle] AS j
        ON j.[JobTitleID] = TRY_CAST(s.[title_id] AS UNIQUEIDENTIFIER)
) AS k;

GO