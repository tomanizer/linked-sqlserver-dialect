IF DB_ID('RemoteDb') IS NULL
BEGIN
  CREATE DATABASE [RemoteDb];
END
GO

USE [RemoteDb];
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'example_table' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
  CREATE TABLE [dbo].[example_table] (
    [id] INT NOT NULL,
    [name] NVARCHAR(50) NULL
  );
END
GO

IF OBJECT_ID(N'dbo.example_view', N'V') IS NULL
BEGIN
  EXEC(N'CREATE VIEW dbo.example_view AS SELECT id, name FROM dbo.example_table;');
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = 'readonly_user')
BEGIN
  CREATE LOGIN [readonly_user] WITH PASSWORD = 'ReadOnly(!)Pass123';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'readonly_user')
BEGIN
  CREATE USER [readonly_user] FOR LOGIN [readonly_user];
END
GO

GRANT SELECT TO [readonly_user];
GO


