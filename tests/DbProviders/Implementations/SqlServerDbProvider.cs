using System.Data;
using System.Text.Json;
using Ormamu;
using DbUp;
using DbUp.Engine;
using Microsoft.Data.SqlClient;
using OrmamuTests.Entities;

namespace OrmamuTests.DbProviders.Implementations;

public class SqlServerDbProvider: IDbProvider
{
    public SqlServerDbProvider()
    {
        Options = TestsConfig.DbOptions[SqlDialect.SqlServer];
        
        SchemaName = TestsConfig.SchemaName;
        
        ReadTestsTableName = TestsConfig.ReadTestsTableName;
        
        CreateTestsTableName = TestsConfig.CreateTestsTableName;
        
        UpdateTestsTableName = TestsConfig.UpdateTestsTableName;
        
        DeleteTestsTableName = TestsConfig.DeleteTestsTableName;
        
        CompositeKeyTestsTableName = TestsConfig.CompositeKeyTestsTableName;
        
        AutoincrementingCompositeKeyTestsTableName = TestsConfig.AutoIncrementingCompositeKeyTestsTableName;
    }
    
    private const string ConnectionString =
        "Data Source=localhost,1434;TrustServerCertificate=true;Initial Catalog=OrmamuTests;" +
        "User ID=sa;Password=Test123!;";

    public IDbConnection GetConnection()
        => new SqlConnection(ConnectionString);

    public OrmamuOptions Options { get; }
    
    private string SchemaName { get; }
    private string ReadTestsTableName { get; }
    private string CreateTestsTableName { get; }
    private string UpdateTestsTableName { get; }
    private string DeleteTestsTableName { get; }
    private string CompositeKeyTestsTableName { get; }
    private string AutoincrementingCompositeKeyTestsTableName { get; }

    public void DeployTestData()
    {
        Stream? testDataStream = typeof(IDbProvider)
            .Assembly
            .GetManifestResourceStream("OrmamuTests.Data.TestData.json");

        Goblin[] sampleData =  testDataStream is not null ? 
            JsonSerializer.Deserialize<Goblin[]>(testDataStream)!:
            [];

        using IDbConnection connection = GetConnection();
        connection.BulkInsert(sampleData);
    }

    public void WipeCreateTestsData()
    {
        using SqlConnection connection = new(ConnectionString);
        connection.Open();
        using SqlCommand command = new(
            $"""
             TRUNCATE TABLE "{SchemaName}"."{CreateTestsTableName}";
             """, connection);
        command.ExecuteNonQuery();
    }

    public void DeploySchema()
    {
        EnsureDatabase.For.SqlDatabase(ConnectionString);
        DropSchema();
        EnsureSchema();
        object[] scriptPayload =
        [
            SchemaName,
            ReadTestsTableName,
            CreateTestsTableName,
            UpdateTestsTableName,
            DeleteTestsTableName,
            CompositeKeyTestsTableName,
            AutoincrementingCompositeKeyTestsTableName,
        ];

        UpgradeEngine engine =
            DeployChanges.To
                .SqlDatabase(ConnectionString)
                .JournalToSqlTable(SchemaName, "SchemaVersions")
                .WithScript(new SqlScript(
                    "Deploy",
                    string.Format(_deployScript, scriptPayload)))
                .LogToConsole()
                .Build();

        DatabaseUpgradeResult result = engine.PerformUpgrade();

        if (!result.Successful)
        {
            Console.WriteLine(
                $"Error occured when applying script {result.ErrorScript.Name} " +
                $"with content:\n\n{result.ErrorScript.Contents}");
            throw result.Error;
        }

        Console.WriteLine("Schema deploy successful!");
    }

    private void EnsureSchema()
    {
        using SqlConnection connection = new(ConnectionString);
        connection.Open();
        
        using SqlCommand command = new(
            $"CREATE SCHEMA \"{SchemaName}\"", connection);
        
        command.ExecuteNonQuery();
        connection.Close();
    }

    private void DropSchema()
    {
        using SqlConnection connection = new(ConnectionString);
        connection.Open();
        using SqlCommand command = new($"""
                                        IF EXISTS (
                                            SELECT 1
                                            FROM INFORMATION_SCHEMA.SCHEMATA
                                            WHERE SCHEMA_NAME = '{SchemaName}'
                                        )
                                        BEGIN
                                        
                                            DECLARE @SchemaName SYSNAME = '{SchemaName}';
                                            DECLARE @Sql NVARCHAR(MAX) = '';
                                            
                                            SELECT @Sql += '
                                            ALTER TABLE [' + s.name + '].[' + t.name + '] DROP CONSTRAINT [' + fk.name + '];'
                                            FROM sys.foreign_keys fk
                                            JOIN sys.tables t ON fk.parent_object_id = t.object_id
                                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName;
                                            
                                            SELECT @Sql += '
                                            DROP TABLE [' + s.name + '].[' + t.name + '];'
                                            FROM sys.tables t
                                            JOIN sys.schemas s ON t.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName;
                                            
                                            SELECT @Sql += '
                                            DROP VIEW [' + s.name + '].[' + v.name + '];'
                                            FROM sys.views v
                                            JOIN sys.schemas s ON v.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName;
                                            
                                            SELECT @Sql += '
                                            DROP PROCEDURE [' + s.name + '].[' + p.name + '];'
                                            FROM sys.procedures p
                                            JOIN sys.schemas s ON p.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName;
                                            
                                            SELECT @Sql += '
                                            DROP FUNCTION [' + s.name + '].[' + f.name + '];'
                                            FROM sys.objects f
                                            JOIN sys.schemas s ON f.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName AND f.type IN ('FN', 'IF', 'TF');
                                            
                                            SELECT @Sql += '
                                            DROP SYNONYM [' + s.name + '].[' + sn.name + '];'
                                            FROM sys.synonyms sn
                                            JOIN sys.schemas s ON sn.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName;
                                            
                                            SELECT @Sql += '
                                            DROP SEQUENCE [' + s.name + '].[' + seq.name + '];'
                                            FROM sys.sequences seq
                                            JOIN sys.schemas s ON seq.schema_id = s.schema_id
                                            WHERE s.name = @SchemaName;
                                            
                                            EXEC sp_executesql @Sql;
                                            
                                            SET @Sql = 'DROP SCHEMA [' + @SchemaName + '];';
                                            EXEC sp_executesql @Sql;
                                        
                                        END
                                        """, connection);
        command.ExecuteNonQuery();
    }

    private readonly string _deployScript = 
        """
        CREATE TABLE {0}.{1} (
          Id int IDENTITY(1,1) NOT NULL,
          Name nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
          FavouriteLetter nvarchar(1) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
          Age smallint NOT NULL,
          Stamina tinyint NOT NULL,
          MagicPower bigint NOT NULL,
          Strength real NOT NULL,
          Agility float NOT NULL,
          Salary decimal(18,2) NOT NULL,
          IsActive bit NOT NULL,
          DateOfBirth datetime2 NOT NULL,
          HobbitAncestry bit NULL,
          CONSTRAINT PK_{1} PRIMARY KEY (Id),
        );
        CREATE TABLE {0}.{2} (
          Id int IDENTITY(1,1) NOT NULL,
          Name nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
          Strength real NOT NULL,
          Height smallint NOT NULL,
          IsActive bit NOT NULL,
          HobbitAncestry bit NULL,
          CONSTRAINT PK_{2} PRIMARY KEY (Id)
        );
        CREATE TABLE {0}.{3} (
          Id nvarchar(50) NOT NULL,
          Name nvarchar(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
          Strength real NOT NULL,
          Height smallint NOT NULL,
          IsActive bit NOT NULL,
          CONSTRAINT PK_{3} PRIMARY KEY (Id)
        );
        CREATE TABLE {0}.{4} (
          Id bigint IDENTITY(1,1) NOT NULL,
          MagicPower bigint NOT NULL,
          DateOfBirth datetime2 NOT NULL,
          CONSTRAINT PK_{4} PRIMARY KEY (Id)
        );
        CREATE TABLE "{0}"."{5}" (
          "Id" int NOT NULL,
          "Name" nvarchar(100) NOT NULL,
          "Personality" smallint NOT NULL,
          CONSTRAINT "PK_{5}" PRIMARY KEY ("Id", "Name")
        );
        CREATE TABLE "{0}"."{6}" (
          "Id" bigint IDENTITY(1,1) NOT NULL,
          "Name" nvarchar(100) NOT NULL,
          "Personality" smallint NOT NULL,
          CONSTRAINT "PK_{6}" PRIMARY KEY ("Id", "Name")
        );
        """;
}