-- Builds the facts from the current employee versions, inside one transaction.
--   * FactEmployeeSnapshot: the current state only (one row per live employee).
--     The existing report sums it, so it must never hold more than one day.
--   * FactEmployeeDailySnapshot: one row per live employee per day, kept for
--     headcount and salary trends. Re-running on the same day replaces that day.
-- Every current employee is included: unknown departments and titles are -1.
CREATE PROC gold.sp_fctTableCreation
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @today DATE = CAST(SYSUTCDATETIME() AS DATE);

    BEGIN TRY
        BEGIN TRANSACTION;

        DELETE FROM gold.FactEmployeeSnapshot;

        INSERT INTO gold.FactEmployeeSnapshot
            (SnapshotDate, EmployeeKey, DepartmentKey, JobTitleKey, Salary, EmployeeCount)
        SELECT
            @today, e.EmployeeKey, e.DepartmentKey, e.JobTitleKey, s.[Salary], 1
        FROM gold.DimEmployee AS e
        JOIN silver.employees AS s
            ON TRY_CAST(s.[id] AS UNIQUEIDENTIFIER) = e.EmployeeSourceID
        WHERE e.IsCurrent = 1;

        DELETE FROM gold.FactEmployeeDailySnapshot WHERE SnapshotDate = @today;

        INSERT INTO gold.FactEmployeeDailySnapshot
            (SnapshotDate, EmployeeKey, DepartmentKey, JobTitleKey, Salary, EmployeeCount)
        SELECT SnapshotDate, EmployeeKey, DepartmentKey, JobTitleKey, Salary, EmployeeCount
        FROM gold.FactEmployeeSnapshot;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH
END

GO