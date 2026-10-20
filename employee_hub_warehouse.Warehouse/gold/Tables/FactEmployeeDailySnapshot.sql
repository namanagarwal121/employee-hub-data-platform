CREATE TABLE [gold].[FactEmployeeDailySnapshot] (
    [SnapshotDate]  DATE            NOT NULL,
    [EmployeeKey]   INT             NOT NULL,
    [DepartmentKey] INT             NOT NULL,
    [JobTitleKey]   INT             NOT NULL,
    [Salary]        DECIMAL (12, 2) NULL,
    [EmployeeCount] INT             NOT NULL
);


GO

