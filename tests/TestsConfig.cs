
using Ormamu;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace OrmamuTests;
public static class TestsConfig
{
    public const SqlDialect DbVariant = SqlDialect.PostgreSql;
    public const string SchemaName = "Tests";
    public const string SecondarySchemaName = "MulticonfigTests";
    public const string ReadTestsTableName = "Goblins";
    public const string CreateTestsTableName = "Dwarves";
    public const string UpdateTestsTableName = "Gnomes";
    public const string DeleteTestsTableName = "Pixies";
    public const string CompositeKeyTestsTableName = "Thronglets";
    public const string AutoincrementingCompositeKeyTestsTableName = "Gremlins";
    public const string MulticonfigTestsTableName = "Imps";
    public const string CustomColumnName = "ImeNaImp";

    public static Dictionary<SqlDialect, OrmamuOptions> DbOptions { get; } = new()
    {
        {
            SqlDialect.PostgreSql, new()
            {
                ConfigId = SqlDialect.PostgreSql,
                Dialect = SqlDialect.PostgreSql,
                NameConverter = NameConverters.ToSnakeCase,
            }
        },
        {
            SqlDialect.SqlServer, new()
            {
                ConfigId = SqlDialect.SqlServer,
                Dialect = SqlDialect.SqlServer,
            }
        },
        {
            SqlDialect.MySql, new()
            {
                ConfigId = SqlDialect.MySql,
                Dialect = SqlDialect.MySql,
                NameConverter = NameConverters.ToLowerCase,
            }
        },
        {
            SqlDialect.MariaDb, new()
            {
                ConfigId = SqlDialect.MariaDb,
                Dialect = SqlDialect.MariaDb,
                NameConverter = NameConverters.ToUpperCase,
            }
        },
        {
            SqlDialect.Sqlite, new()
            {
                ConfigId = SqlDialect.Sqlite,
                Dialect = SqlDialect.Sqlite,
            }
        }
    };

    public static OrmamuOptions MulticonfigOptions { get; } = new()
    {
        ConfigId = MulticonfigTestsTableName,
        Dialect = SqlDialect.PostgreSql,
        NameConverter = NameConverters.ToKebabCase,
    };
}