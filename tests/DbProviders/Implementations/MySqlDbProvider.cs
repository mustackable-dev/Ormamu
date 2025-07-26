using System.Data;
using System.Text.Json;
using Ormamu;
using DbUp;
using DbUp.Engine;
using MySqlConnector;
using OrmamuTests.Entities;

namespace OrmamuTests.DbProviders.Implementations;

public class MySqlDbProvider: IDbProvider
{
    public MySqlDbProvider()
    {
        Options = TestsConfig.DbOptions[SqlDialect.MySql];
        
        SchemaName = TestsConfig.SchemaName;
        
        ReadTestsTableName = TestsConfig.ReadTestsTableName;
        
        CreateTestsTableName = TestsConfig.CreateTestsTableName;
        
        UpdateTestsTableName = TestsConfig.UpdateTestsTableName;
        
        DeleteTestsTableName = TestsConfig.DeleteTestsTableName;
        
        CompositeKeyTestsTableName = TestsConfig.CompositeKeyTestsTableName;
    }
    
    private string _connectionString =
        "Server=localhost;Port=3307;Database=sys;Uid=root;Pwd=Test123!;";

    public IDbConnection GetConnection()
        => new MySqlConnection(_connectionString);

    public OrmamuOptions Options { get; }
    
    private string SchemaName { get; }
    private string ReadTestsTableName { get; }
    private string CreateTestsTableName { get; }
    private string UpdateTestsTableName { get; }
    private string DeleteTestsTableName { get; }
    private string CompositeKeyTestsTableName { get; }

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
        using MySqlConnection connection = new(_connectionString);
        connection.Open();
        using MySqlCommand command = new(
            $"""
             TRUNCATE TABLE `{CreateTestsTableName}`;
             """, connection);
        command.ExecuteNonQuery();
    }

    public void DeploySchema()
    {
        DropSchema();
        _connectionString = _connectionString.Replace("sys", "OrmamuTests");
        EnsureDatabase.For.MySqlDatabase(_connectionString);
        object[] scriptPayload =
        [
            SchemaName,
            ReadTestsTableName,
            CreateTestsTableName,
            UpdateTestsTableName,
            DeleteTestsTableName,
            CompositeKeyTestsTableName
        ];

        UpgradeEngine engine =
            DeployChanges.To
                .MySqlDatabase(_connectionString)
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

    private void DropSchema()
    {
        using MySqlConnection connection = new(_connectionString);
        connection.Open();
        using MySqlCommand command = new($"DROP DATABASE IF EXISTS OrmamuTests", connection);
        command.ExecuteNonQuery();
    }

    private readonly string _deployScript = 
        """
        CREATE TABLE `{1}` (
          `id` INT NOT NULL AUTO_INCREMENT,
          `name` TEXT NOT NULL,
          `favouriteletter` CHAR(1) NOT NULL,
          `age` SMALLINT NOT NULL,
          `stamina` SMALLINT NOT NULL,
          `magicpower` BIGINT NOT NULL,
          `strength` FLOAT NOT NULL,
          `agility` DOUBLE NOT NULL,
          `salary` DECIMAL(18,2) NOT NULL,
          `isactive` BOOLEAN NOT NULL,
          `dateofbirth` DATETIME NOT NULL,
          `hobbitancestry` BOOLEAN,
          PRIMARY KEY (`id`)
        );
        
        CREATE TABLE `{2}` (
          `id` INT NOT NULL AUTO_INCREMENT,
          `name` TEXT NOT NULL,
          `strength` FLOAT NOT NULL,
          `height` SMALLINT NOT NULL,
          `isactive` BOOLEAN NOT NULL,
          `hobbitancestry` BOOLEAN,
          PRIMARY KEY (`id`)
        );
        
        CREATE TABLE `{3}` (
          `id` nvarchar(32) NOT NULL,
          `name` TEXT NOT NULL,
          `strength` FLOAT NOT NULL,
          `height` SMALLINT NOT NULL,
          `isactive` BOOLEAN NOT NULL,
          PRIMARY KEY (`id`)
        );
        
        CREATE TABLE `{4}` (
          `id` BIGINT NOT NULL AUTO_INCREMENT,
          `magicpower` BIGINT NOT NULL,
          `dateofbirth` DATETIME NOT NULL,
          PRIMARY KEY (`id`)
        );
        
        CREATE TABLE `{5}` (
          `id` INT NOT NULL,
          `name` VARCHAR(100) NOT NULL,
          `personality` SMALLINT NOT NULL,
          PRIMARY KEY (`id`, `name`)
        );
        """;
}