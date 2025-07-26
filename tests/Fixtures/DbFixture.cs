using System.Text.Json;
using System.Text.Json.Serialization;
using Dapper;
using Ormamu;
using OrmamuTests.Fixtures;
using OrmamuTests.DbProviders;
using OrmamuTests.DbProviders.Implementations;

[assembly: AssemblyFixture(typeof(DbFixture))]

namespace OrmamuTests.Fixtures;
public sealed class DbFixture: IDisposable
{
    public IDbProvider DbProvider { get; }
    public IDbProvider MulticonfigDbProvider { get; }
    public JsonSerializerOptions SerializerOptions { get; }
    public DbFixture()
    {
        PostgreSqlDbProvider postgreSqlProvider = new();
        SqlServerDbProvider sqlServerProvider = new();
        MySqlDbProvider mySqlProvider = new();
        MariaDbProvider mariaDbProvider = new();
        SqliteDbProvider sqliteDbProvider = new();
        MulticonfigTestDbProvider multiconfigProvider = new();
        
        switch (TestsConfig.DbVariant)
        {
            case SqlDialect.PostgreSql:
                DbProvider = postgreSqlProvider;
                break;
            case SqlDialect.SqlServer:
                DbProvider = sqlServerProvider;
                SqlMapper.AddTypeMap(typeof(DateTime), System.Data.DbType.DateTime2);
                break;
            case SqlDialect.MySql:
                DbProvider = mySqlProvider;
                break;
            case SqlDialect.MariaDb:
                DbProvider = mariaDbProvider;
                break;
            case SqlDialect.Sqlite:
                DbProvider = sqliteDbProvider;
                break;
        }
        
        Configuration.Apply([..TestsConfig.DbOptions.Values.ToArray(), TestsConfig.MulticonfigOptions]);
        
        DbProvider.DeploySchema();
        DbProvider.DeployTestData();
        
        MulticonfigDbProvider = multiconfigProvider;
        MulticonfigDbProvider.DeploySchema();
        
        SerializerOptions = new JsonSerializerOptions();
        SerializerOptions.Converters.Add(new  JsonStringEnumConverter());
        SerializerOptions.PropertyNameCaseInsensitive = true;
    }

    public void Dispose()
    {
        //Method left empty intentionally
    }
}