-- Builds the gold dimensions incrementally, inside one transaction.
--   * DimDepartment / DimJobTitle: keys are stable. Existing rows keep their key
--     (names are updated in place), new ids get the next key, and -1 is "Unknown".
--   * DimEmployee: SCD Type 2. A changed employee gets a new version and the old
--     one is closed (EffectiveTo, IsCurrent = 0). A deleted employee's current
--     version is closed with IsDeleted = 1. Nothing is truncated, so history and
--     keys survive every run.
CREATE PROC gold.sp_DimensionTableCreation
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @now DATETIME2(6) = SYSUTCDATETIME();

    BEGIN TRY
        BEGIN TRANSACTION;

        ------------------------------------------------------------ departments
        IF NOT EXISTS (SELECT 1 FROM gold.DimDepartment WHERE DepartmentKey = -1)
            INSERT INTO gold.DimDepartment (DepartmentKey, DepartmentID, DepartmentName)
            VALUES (-1, '00000000-0000-0000-0000-000000000000', 'Unknown');

        UPDATE d
        SET DepartmentName = s.[Name]
        FROM gold.DimDepartment AS d
        JOIN silver.departments AS s
            ON d.DepartmentID = CAST(s.[id] AS UNIQUEIDENTIFIER)
        WHERE ISNULL(d.DepartmentName, '') <> ISNULL(s.[Name], '');

        DECLARE @max_dept INT = ISNULL((SELECT MAX(DepartmentKey) FROM gold.DimDepartment WHERE DepartmentKey > 0), 0);
        INSERT INTO gold.DimDepartment (DepartmentKey, DepartmentID, DepartmentName)
        SELECT
            @max_dept + ROW_NUMBER() OVER (ORDER BY s.[Name], s.[id]),
            CAST(s.[id] AS UNIQUEIDENTIFIER),
            s.[Name]
        FROM silver.departments AS s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.DimDepartment AS d
            WHERE d.DepartmentID = CAST(s.[id] AS UNIQUEIDENTIFIER));

        ------------------------------------------------------------ job titles
        IF NOT EXISTS (SELECT 1 FROM gold.DimJobTitle WHERE JobTitleKey = -1)
            INSERT INTO gold.DimJobTitle (JobTitleKey, JobTitleID, JobTitle)
            VALUES (-1, '00000000-0000-0000-0000-000000000000', 'Unknown');

        UPDATE j
        SET JobTitle = s.[Title]
        FROM gold.DimJobTitle AS j
        JOIN silver.jobtitles AS s
            ON j.JobTitleID = CAST(s.[id] AS UNIQUEIDENTIFIER)
        WHERE ISNULL(j.JobTitle, '') <> ISNULL(s.[Title], '');

        DECLARE @max_title INT = ISNULL((SELECT MAX(JobTitleKey) FROM gold.DimJobTitle WHERE JobTitleKey > 0), 0);
        INSERT INTO gold.DimJobTitle (JobTitleKey, JobTitleID, JobTitle)
        SELECT
            @max_title + ROW_NUMBER() OVER (ORDER BY s.[Title], s.[id]),
            CAST(s.[id] AS UNIQUEIDENTIFIER),
            s.[Title]
        FROM silver.jobtitles AS s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.DimJobTitle AS j
            WHERE j.JobTitleID = CAST(s.[id] AS UNIQUEIDENTIFIER));

        ------------------------------------------------------------ employees (SCD2)
        -- One-time backfill for rows written before SCD2: treat them as current
        -- and hash their own attributes, so unchanged employees get no new version.
        UPDATE gold.DimEmployee
        SET EffectiveFrom = ISNULL(EffectiveFrom, '1900-01-01'),
            IsCurrent     = 1,
            IsDeleted     = 0,
            JobTitleKey   = ISNULL(JobTitleKey, -1),
            DepartmentKey = ISNULL(DepartmentKey, -1)
        WHERE IsCurrent IS NULL;

        UPDATE gold.DimEmployee
        SET RowHash = CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CONCAT(
                ISNULL(FullName, ''), '|',
                ISNULL(Email, ''), '|',
                ISNULL(CONVERT(VARCHAR(10), JoiningDate, 23), ''), '|',
                DepartmentKey, '|',
                JobTitleKey)), 2)
        WHERE RowHash IS NULL;

        -- Close the current version of employees that changed or were deleted.
        UPDATE e
        SET EffectiveTo = @now,
            IsCurrent   = 0,
            IsDeleted   = CASE WHEN s.EmployeeSourceID IS NULL THEN 1 ELSE 0 END
        FROM gold.DimEmployee AS e
        LEFT JOIN gold.vw_EmployeeStage AS s
            ON s.EmployeeSourceID = e.EmployeeSourceID
        WHERE e.IsCurrent = 1
          AND (s.EmployeeSourceID IS NULL OR s.RowHash <> e.RowHash);

        -- Add a current version for new employees and for the ones just closed.
        DECLARE @max_emp INT = ISNULL((SELECT MAX(EmployeeKey) FROM gold.DimEmployee), 0);
        INSERT INTO gold.DimEmployee (
            EmployeeKey, EmployeeID, EmployeeSourceID, FullName, Email, JoiningDate,
            DepartmentKey, JobTitleKey, EffectiveFrom, EffectiveTo, IsCurrent, IsDeleted, RowHash)
        SELECT
            @max_emp + ROW_NUMBER() OVER (ORDER BY s.EmployeeID, s.EmployeeSourceID),
            s.EmployeeID, s.EmployeeSourceID, s.FullName, s.Email, s.JoiningDate,
            s.DepartmentKey, s.JobTitleKey, @now, NULL, 1, 0, s.RowHash
        FROM gold.vw_EmployeeStage AS s
        WHERE NOT EXISTS (
            SELECT 1 FROM gold.DimEmployee AS e
            WHERE e.EmployeeSourceID = s.EmployeeSourceID AND e.IsCurrent = 1);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END

GO