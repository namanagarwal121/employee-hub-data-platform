CREATE TABLE [silver].[employees] (
    [id]           VARCHAR (100)   NOT NULL,
    [CreatedAt]    DATETIME2 (6)   NULL,
    [Email]        VARCHAR (320)   NULL,
    [EmployeeID]   INT             NOT NULL,
    [FullName]     VARCHAR (200)   NULL,
    [JoiningDate]  DATETIME2 (6)   NULL,
    [Salary]       DECIMAL (12, 2) NULL,
    [UpdatedAt]    DATETIME2 (6)   NULL,
    [createdBy_id] VARCHAR (100)   NULL,
    [dept_id]      VARCHAR (100)   NULL,
    [title_id]     VARCHAR (100)   NULL,
    [updatedBy_id] VARCHAR (100)   NULL
);


GO