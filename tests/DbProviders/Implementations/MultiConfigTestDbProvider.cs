using System.Data;
using Ormamu;
using DbUp;
using Npgsql;

namespace OrmamuTests.DbProviders.Implementations;

public class MultiConfigTestDbProvider: IDbProvider
{
    private const string ConnectionString =
        "User ID=postgres;Password=Test123!;Host=localhost;Port=5433;Database=OrmamuTests;" +
        "Pooling=true;Connection Lifetime=0;";

    public IDbConnection GetConnection()
        => new NpgsqlConnection(ConnectionString);

    public OrmamuOptions Options { get; } = TestsConfig.DbOptions[TestsConfig.MultiConfigTestsTableName];

    private string SchemaName => TestsConfig.SecondarySchemaName;
    private string MultiConfigTestsTableName => TestsConfig.MultiConfigTestsTableName;

    public void DeployTestData()
    {
    }

    public void WipeCreateTestsData()
    {
        using NpgsqlConnection connection = new(ConnectionString);
        connection.Open();
        using NpgsqlCommand command = new(
            $"""
             TRUNCATE TABLE "{SchemaName}"."{MultiConfigTestsTableName}";
             """, connection);
        command.ExecuteNonQuery();
    }

    public void DeploySchema()
    {
        EnsureDatabase.For.PostgresqlDatabase(ConnectionString);
        DropSchema();
        EnsureSchema();
        object[] scriptPayload =
        [
            SchemaName,
            MultiConfigTestsTableName,
            TestsConfig.CustomColumnName
        ];
        
        using NpgsqlConnection connection = new(ConnectionString);
        connection.Open();

        string tableCreateScript = string.Format(_deployScript, scriptPayload);
        
        using NpgsqlCommand command = new(tableCreateScript, connection);
        
        command.ExecuteNonQuery();
        connection.Close();
        
        Console.WriteLine("Schema deploy successful!");
    }

    private void EnsureSchema()
    {
        using NpgsqlConnection connection = new(ConnectionString);
        connection.Open();
        
        using NpgsqlCommand command = new(
            $"CREATE SCHEMA IF NOT EXISTS \"{SchemaName}\"", connection);
        
        command.ExecuteNonQuery();
        connection.Close();
    }

    private void DropSchema()
    {
        using NpgsqlConnection connection = new(ConnectionString);
        connection.Open();
        using NpgsqlCommand command = new($"DROP SCHEMA IF EXISTS \"{SchemaName}\" CASCADE", connection);
        command.ExecuteNonQuery();
    }

    private readonly string _deployScript = 
        """
        CREATE TABLE "{0}"."{1}" (
          "guid-key" uuid NOT NULL,
          "{2}" text NOT NULL,
          "favourite-letter" bpchar(1) NOT NULL,
          "age" int2 NOT NULL,
          CONSTRAINT "PK_{1}" PRIMARY KEY ("guid-key")
        );
        """;
}