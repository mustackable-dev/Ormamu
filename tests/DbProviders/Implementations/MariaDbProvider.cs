using System.Data;
using System.Text.Json;
using Ormamu;
using DbUp;
using DbUp.Engine;
using MySqlConnector;
using OrmamuTests.Entities;

namespace OrmamuTests.DbProviders.Implementations;

public class MariaDbProvider: IDbProvider
{
    public MariaDbProvider()
    {
        Options = TestsConfig.DbOptions[SqlDialect.MariaDb];
        
        SchemaName = TestsConfig.SchemaName;
        
        ReadTestsTableName = TestsConfig.ReadTestsTableName;
        
        CreateTestsTableName = TestsConfig.CreateTestsTableName;
        
        UpdateTestsTableName = TestsConfig.UpdateTestsTableName;
        
        DeleteTestsTableName = TestsConfig.DeleteTestsTableName;
        
        CompositeKeyTestsTableName = TestsConfig.CompositeKeyTestsTableName;
        
        AutoincrementingCompositeKeyTestsTableName = TestsConfig.AutoIncrementingCompositeKeyTestsTableName;
    }
    
    private string _connectionString =
        "Server=localhost;Port=3305;Database=sys;Uid=root;Pwd=Test123!;";

    public IDbConnection GetConnection()
        => new MySqlConnection(_connectionString);

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
            CompositeKeyTestsTableName,
            AutoincrementingCompositeKeyTestsTableName,
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
          `ID` INT NOT NULL AUTO_INCREMENT,
          `NAME` TEXT NOT NULL,
          `FAVOURITELETTER` CHAR(1) NOT NULL,
          `AGE` SMALLINT NOT NULL,
          `STAMINA` SMALLINT NOT NULL,
          `MAGICPOWER` BIGINT NOT NULL,
          `STRENGTH` FLOAT NOT NULL,
          `AGILITY` DOUBLE NOT NULL,
          `SALARY` DECIMAL(18,2) NOT NULL,
          `ISACTIVE` BOOLEAN NOT NULL,
          `DATEOFBIRTH` DATETIME NOT NULL,
          `HOBBITANCESTRY` BOOLEAN,
          PRIMARY KEY (`ID`)
        );
        
        CREATE TABLE `{2}` (
          `ID` INT NOT NULL AUTO_INCREMENT,
          `NAME` TEXT NOT NULL,
          `STRENGTH` FLOAT NOT NULL,
          `HEIGHT` SMALLINT NOT NULL,
          `ISACTIVE` BOOLEAN NOT NULL,
          `HOBBITANCESTRY` BOOLEAN,
          PRIMARY KEY (`ID`)
        );
        
        CREATE TABLE `{3}` (
          `ID` NVARCHAR(32) NOT NULL,
          `NAME` TEXT NOT NULL,
          `STRENGTH` FLOAT NOT NULL,
          `HEIGHT` SMALLINT NOT NULL,
          `ISACTIVE` BOOLEAN NOT NULL,
          PRIMARY KEY (`ID`)
        );
        
        CREATE TABLE `{4}` (
          `ID` BIGINT NOT NULL AUTO_INCREMENT,
          `MAGICPOWER` BIGINT NOT NULL,
          `DATEOFBIRTH` DATETIME NOT NULL,
          PRIMARY KEY (`ID`)
        );
        
        CREATE TABLE `{5}` (
          `ID` INT NOT NULL,
          `NAME` VARCHAR(200) NOT NULL,
          `PERSONALITY` SMALLINT NOT NULL,
          PRIMARY KEY (`ID`, `NAME`)
        );
        
        CREATE TABLE `{6}` (
          `ID` BIGINT NOT NULL AUTO_INCREMENT,
          `NAME` VARCHAR(200) NOT NULL,
          `PERSONALITY` SMALLINT NOT NULL,
          PRIMARY KEY (`ID`, `NAME`)
        );
        """;
}