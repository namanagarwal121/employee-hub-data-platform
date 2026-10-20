CREATE TABLE [gold].[DimEmployee] (
    [EmployeeKey]      INT              NOT NULL,
    [EmployeeID]       INT              NOT NULL,
    [EmployeeSourceID] UNIQUEIDENTIFIER NOT NULL,
    [FullName]         VARCHAR (200)    NULL,
    [Email]            VARCHAR (200)    NULL,
    [JoiningDate]      DATE             NULL,
    [DepartmentKey]    INT              NULL,
    [JobTitleKey]      INT              NULL,
    [EffectiveFrom]    DATETIME2 (6)    NULL,
    [EffectiveTo]      DATETIME2 (6)    NULL,
    [IsCurrent]        BIT              NULL,
    [IsDeleted]        BIT              NULL,
    [RowHash]          VARCHAR (64)     NULL
);


GO

