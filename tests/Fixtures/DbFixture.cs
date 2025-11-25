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
    public DbFixture()
    {
        PostgreSqlDbProvider postgreSqlProvider = new();
        SqlServerDbProvider sqlServerProvider = new();
        MySqlDbProvider mySqlProvider = new();
        MariaDbProvider mariaDbProvider = new();
        SqliteDbProvider sqliteDbProvider = new();
        MultiConfigTestDbProvider multiConfigProvider = new();
        
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
        
        OrmamuConfig.Apply(TestsConfig.DbOptions);
        
        DbProvider.DeploySchema();
        DbProvider.DeployTestData();
        
        MulticonfigDbProvider = multiConfigProvider;
        MulticonfigDbProvider.DeploySchema();
    }

    public void Dispose()
    {
        //Method left empty intentionally
    }
}