/*
  Employee Hub · stream apply objects for the APP's Fabric SQL database
  ---------------------------------------------------------------------
  Run once in the app's SQL database (Fabric portal > the app's SQL database > New query).
  Safe to re-run: every object is created only if missing, and the procedure is CREATE OR ALTER.

  The pipeline copies the lakehouse tables lh_employee_hub.sql_ready.* into the staging
  tables below, then calls stream_stage.sp_apply_stream_changes, which applies them to the
  app tables in ONE transaction: everything lands, or nothing does.

  Everything lives in its own schema, stream_stage, so the app's dbo tables keep only what
  the app defines. If a future app deploy ever removes this schema, run this script again.
*/

IF SCHEMA_ID(N'stream_stage') IS NULL EXEC(N'CREATE SCHEMA stream_stage');
GO

IF OBJECT_ID(N'stream_stage.AppUsers_insert') IS NULL
CREATE TABLE stream_stage.AppUsers_insert (
    id           UNIQUEIDENTIFIER NOT NULL,
    Email        NVARCHAR(254)    NOT NULL,
    FirstSeenAt  DATETIME2        NOT NULL
);

IF OBJECT_ID(N'stream_stage.Departments_insert') IS NULL
CREATE TABLE stream_stage.Departments_insert (
    id    UNIQUEIDENTIFIER NOT NULL,
    Name  NVARCHAR(100)    NOT NULL
);

IF OBJECT_ID(N'stream_stage.JobTitles_insert') IS NULL
CREATE TABLE stream_stage.JobTitles_insert (
    id     UNIQUEIDENTIFIER NOT NULL,
    Title  NVARCHAR(150)    NOT NULL
);

IF OBJECT_ID(N'stream_stage.Employees_insert') IS NULL
CREATE TABLE stream_stage.Employees_insert (
    id            UNIQUEIDENTIFIER NOT NULL,
    EmployeeID    INT              NOT NULL,
    FullName      NVARCHAR(150)    NOT NULL,
    Email         NVARCHAR(254)    NOT NULL,
    Salary        DECIMAL(12,2)    NOT NULL,
    JoiningDate   DATETIME2        NOT NULL,
    CreatedAt     DATETIME2        NOT NULL,
    UpdatedAt     DATETIME2        NULL,
    dept_id       UNIQUEIDENTIFIER NOT NULL,
    title_id      UNIQUEIDENTIFIER NOT NULL,
    createdBy_id  UNIQUEIDENTIFIER NULL,
    updatedBy_id  UNIQUEIDENTIFIER NULL
);

IF OBJECT_ID(N'stream_stage.Employees_update') IS NULL
CREATE TABLE stream_stage.Employees_update (
    id            UNIQUEIDENTIFIER NOT NULL,
    EmployeeID    INT              NOT NULL,
    FullName      NVARCHAR(150)    NOT NULL,
    Email         NVARCHAR(254)    NOT NULL,
    Salary        DECIMAL(12,2)    NOT NULL,
    JoiningDate   DATETIME2        NOT NULL,
    CreatedAt     DATETIME2        NOT NULL,
    UpdatedAt     DATETIME2        NULL,
    dept_id       UNIQUEIDENTIFIER NOT NULL,
    title_id      UNIQUEIDENTIFIER NOT NULL,
    createdBy_id  UNIQUEIDENTIFIER NULL,
    updatedBy_id  UNIQUEIDENTIFIER NULL
);

IF OBJECT_ID(N'stream_stage.Employees_delete') IS NULL
CREATE TABLE stream_stage.Employees_delete (
    id          UNIQUEIDENTIFIER NOT NULL,
    EmployeeID  INT              NOT NULL
);

IF OBJECT_ID(N'stream_stage.AuditEvents_insert') IS NULL
CREATE TABLE stream_stage.AuditEvents_insert (
    id            UNIQUEIDENTIFIER NOT NULL,
    OccurredAt    DATETIME2        NOT NULL,
    UserEmail     NVARCHAR(254)    NOT NULL,
    Action        NVARCHAR(60)     NOT NULL,
    EntityType    NVARCHAR(40)     NOT NULL,
    RecordId      NVARCHAR(64)     NULL,
    Status        NVARCHAR(20)     NOT NULL,
    Details       NVARCHAR(2000)   NULL,
    ErrorDetails  NVARCHAR(1000)   NULL
);

-- One row per pipeline apply, so you can see what each run changed.
IF OBJECT_ID(N'stream_stage.apply_log') IS NULL
CREATE TABLE stream_stage.apply_log (
    applied_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    app_users_added    INT NOT NULL,
    departments_added  INT NOT NULL,
    job_titles_added   INT NOT NULL,
    employees_added    INT NOT NULL,
    employees_updated  INT NOT NULL,
    employees_deleted  INT NOT NULL,
    audit_rows_added   INT NOT NULL
);
GO

CREATE OR ALTER PROCEDURE stream_stage.sp_apply_stream_changes
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @users INT, @depts INT, @titles INT, @added INT, @updated INT, @deleted INT, @audit INT;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. The hr-feed user that stream rows are attributed to.
        INSERT INTO dbo.AppUsers (id, Email, FirstSeenAt)
        SELECT s.id, s.Email, s.FirstSeenAt
        FROM stream_stage.AppUsers_insert s
        WHERE NOT EXISTS (SELECT 1 FROM dbo.AppUsers u WHERE u.id = s.id OR u.Email = s.Email);
        SET @users = @@ROWCOUNT;

        -- 2. Lookups (only filled when the notebook runs with create_missing_lookups = True).
        INSERT INTO dbo.Departments (id, Name)
        SELECT s.id, s.Name
        FROM stream_stage.Departments_insert s
        WHERE NOT EXISTS (SELECT 1 FROM dbo.Departments d WHERE d.id = s.id OR d.Name = s.Name);
        SET @depts = @@ROWCOUNT;

        INSERT INTO dbo.JobTitles (id, Title)
        SELECT s.id, s.Title
        FROM stream_stage.JobTitles_insert s
        WHERE NOT EXISTS (SELECT 1 FROM dbo.JobTitles t WHERE t.id = s.id OR t.Title = s.Title);
        SET @titles = @@ROWCOUNT;

        -- 3. New employees. Skips rows that already landed (rerun before replication caught up).
        INSERT INTO dbo.Employees (id, EmployeeID, FullName, Email, Salary, JoiningDate, CreatedAt,
                                   UpdatedAt, dept_id, title_id, createdBy_id, updatedBy_id)
        SELECT s.id, s.EmployeeID, s.FullName, s.Email, s.Salary, s.JoiningDate, s.CreatedAt,
               s.UpdatedAt, s.dept_id, s.title_id, s.createdBy_id, s.updatedBy_id
        FROM stream_stage.Employees_insert s
        WHERE NOT EXISTS (SELECT 1 FROM dbo.Employees e WHERE e.id = s.id OR e.EmployeeID = s.EmployeeID);
        SET @added = @@ROWCOUNT;

        -- 4. Changes. Never moves a row backwards: a newer edit made in the app wins.
        UPDATE e
        SET e.FullName     = s.FullName,
            e.Email        = s.Email,
            e.Salary       = s.Salary,
            e.JoiningDate  = s.JoiningDate,
            e.UpdatedAt    = s.UpdatedAt,
            e.dept_id      = s.dept_id,
            e.title_id     = s.title_id,
            e.updatedBy_id = s.updatedBy_id
        FROM dbo.Employees e
        JOIN stream_stage.Employees_update s ON s.id = e.id
        WHERE s.UpdatedAt > COALESCE(e.UpdatedAt, e.CreatedAt);
        SET @updated = @@ROWCOUNT;

        -- 5. Leavers.
        DELETE e
        FROM dbo.Employees e
        JOIN stream_stage.Employees_delete s ON s.id = e.id AND s.EmployeeID = e.EmployeeID;
        SET @deleted = @@ROWCOUNT;

        -- 6. Audit trail, as the app records its own changes.
        INSERT INTO dbo.AuditEvents (id, OccurredAt, UserEmail, Action, EntityType, RecordId,
                                     Status, Details, ErrorDetails)
        SELECT s.id, s.OccurredAt, s.UserEmail, s.Action, s.EntityType, s.RecordId,
               s.Status, s.Details, s.ErrorDetails
        FROM stream_stage.AuditEvents_insert s
        WHERE NOT EXISTS (SELECT 1 FROM dbo.AuditEvents a WHERE a.id = s.id);
        SET @audit = @@ROWCOUNT;

        INSERT INTO stream_stage.apply_log (app_users_added, departments_added, job_titles_added,
                                            employees_added, employees_updated, employees_deleted,
                                            audit_rows_added)
        VALUES (@users, @depts, @titles, @added, @updated, @deleted, @audit);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;

    SELECT @users AS app_users_added, @depts AS departments_added, @titles AS job_titles_added,
           @added AS employees_added, @updated AS employees_updated, @deleted AS employees_deleted,
           @audit AS audit_rows_added;
END;
GO
