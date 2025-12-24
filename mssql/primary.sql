DECLARE @ls sysname = 'LS_REMOTE';

IF NOT EXISTS (SELECT 1 FROM sys.servers WHERE name = @ls)
BEGIN
  -- NOTE: Linked server providers can vary by SQL Server/Linux image.
  -- This is the simplest form; if it fails, see README troubleshooting.
  EXEC master.dbo.sp_addlinkedserver
    @server     = @ls,
    @srvproduct = N'',
    @provider   = N'MSOLEDBSQL',
    @datasrc    = N'sql2';
END
GO

-- Map all local logins to the remote readonly login.
EXEC master.dbo.sp_addlinkedsrvlogin
  @rmtsrvname = N'LS_REMOTE',
  @useself = N'False',
  @locallogin = NULL,
  @rmtuser = N'readonly_user',
  @rmtpassword = N'ReadOnly(!)Pass123';
GO


