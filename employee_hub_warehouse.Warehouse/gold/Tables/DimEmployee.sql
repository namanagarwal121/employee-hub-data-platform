CREATE TABLE [gold].[DimEmployee] (
    [EmployeeKey]      INT              NOT NULL,
    [EmployeeID]       INT              NOT NULL,
    [EmployeeSourceID] UNIQUEIDENTIFIER NOT NULL,
    [FullName]         VARCHAR (200)    NULL,
    [Email]            VARCHAR (200)    NULL,
    [JoiningDate]      DATE             NULL,
    [DepartmentKey]    INT              NULL,
    [JobTitleKey]      INT              NULL
);


GO