using System.Data;
using System.Text.Json;
using Dapper;
using Ormamu;
using Microsoft.Data.Sqlite;
using OrmamuTests.Entities;

namespace OrmamuTests.DbProviders.Implementations;

public class SqliteDbProvider: IDbProvider
{
    public SqliteDbProvider()
    {
        Options = TestsConfig.DbOptions[SqlDialect.Sqlite];
        
        ReadTestsTableName = TestsConfig.ReadTestsTableName;
        
        CreateTestsTableName = TestsConfig.CreateTestsTableName;
        
        UpdateTestsTableName = TestsConfig.UpdateTestsTableName;
        
        DeleteTestsTableName = TestsConfig.DeleteTestsTableName;
        
        CompositeKeyTestsTableName = TestsConfig.CompositeKeyTestsTableName;
        
        AutoincrementingCompositeKeyTestsTableName = TestsConfig.AutoIncrementingCompositeKeyTestsTableName;
    }

    private const string ConnectionString =
        "DataSource=file::memory:?cache=shared";

    public IDbConnection GetConnection()
        => new SqliteConnection(ConnectionString);

    public OrmamuOptions Options { get; }
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
        using SqliteConnection connection = new(ConnectionString);
        connection.Open();
        using SqliteCommand command = new(
            $"""
             DELETE FROM {CreateTestsTableName};
             """, connection);
        command.ExecuteNonQuery();
    }

    public void DeploySchema()
    {
        SQLitePCL.Batteries.Init();
        object[] scriptPayload =
        [
            ReadTestsTableName,
            CreateTestsTableName,
            UpdateTestsTableName,
            DeleteTestsTableName,
            CompositeKeyTestsTableName,
            AutoincrementingCompositeKeyTestsTableName,
        ];
        
        string deployScript = string.Format(_deployScript, scriptPayload);
        
        using IDbConnection connection = GetConnection();

        connection.Execute(deployScript);

        Console.WriteLine("Schema deploy successful!");
    }

    private readonly string _deployScript = 
        """
        CREATE TABLE {0} (
          Id INTEGER PRIMARY KEY AUTOINCREMENT,
          Name TEXT NOT NULL,
          FavouriteLetter TEXT NOT NULL CHECK(length(FavouriteLetter) = 1),
          Age INTEGER NOT NULL,
          Stamina INTEGER NOT NULL,
          MagicPower INTEGER NOT NULL,
          Strength REAL NOT NULL,
          Agility REAL NOT NULL,
          Salary NUMERIC NOT NULL,
          IsActive INTEGER NOT NULL,
          DateOfBirth TEXT NOT NULL,
          HobbitAncestry INTEGER
        );
        
        CREATE TABLE {1} (
          Id INTEGER PRIMARY KEY AUTOINCREMENT,
          Name TEXT NOT NULL,
          Strength REAL NOT NULL,
          Height INTEGER NOT NULL,
          IsActive INTEGER NOT NULL,
          HobbitAncestry INTEGER
        );
        
        CREATE TABLE {2} (
          Id TEXT PRIMARY KEY,
          Name TEXT NOT NULL,
          Strength REAL NOT NULL,
          Height INTEGER NOT NULL,
          IsActive INTEGER NOT NULL
        );
        
        CREATE TABLE {3} (
          Id INTEGER PRIMARY KEY AUTOINCREMENT,
          MagicPower INTEGER NOT NULL,
          DateOfBirth TEXT NOT NULL
        );
        CREATE TABLE {4} (
          Id INTEGER NOT NULL,
          Name TEXT NOT NULL,
          Personality INTEGER NOT NULL,
          PRIMARY KEY (Id, Name)
        );
        CREATE TABLE {5} (
          Id INTEGER PRIMARY KEY AUTOINCREMENT,
          Name TEXT NOT NULL,
          Personality INTEGER NOT NULL,
          UNIQUE (Id, Name)
        );
        """;
}