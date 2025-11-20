
using Ormamu;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace OrmamuTests;
public static class TestsConfig
{
    public const SqlDialect DbVariant = SqlDialect.MariaDb;
    public const string SchemaName = "Tests";
    public const string SecondarySchemaName = "MulticonfigTests";
    public const string ReadTestsTableName = "Goblins";
    public const string CreateTestsTableName = "Dwarves";
    public const string UpdateTestsTableName = "Gnomes";
    public const string DeleteTestsTableName = "Pixies";
    public const string CompositeKeyTestsTableName = "Thronglets";
    public const string AutoIncrementingCompositeKeyTestsTableName = "Gremlins";
    public const string MultiConfigTestsTableName = "Imps";
    public const string CustomColumnName = "ImeNaImp";

    public static Dictionary<object, OrmamuOptions> DbOptions { get; } = new()
    {
        {
            SqlDialect.PostgreSql, new()
            {
                Dialect = SqlDialect.PostgreSql,
                NameConverter = NameConverters.ToSnakeCase,
            }
        },
        {
            SqlDialect.SqlServer, new()
            {
                Dialect = SqlDialect.SqlServer,
            }
        },
        {
            SqlDialect.MySql, new()
            {
                Dialect = SqlDialect.MySql,
                NameConverter = NameConverters.ToLowerCase,
            }
        },
        {
            SqlDialect.MariaDb, new()
            {
                Dialect = SqlDialect.MariaDb,
                NameConverter = NameConverters.ToUpperCase,
            }
        },
        {
            SqlDialect.Sqlite, new()
            {
                Dialect = SqlDialect.Sqlite,
            }
        },
        {
            MultiConfigTestsTableName, new()
            {
                Dialect = SqlDialect.PostgreSql,
                NameConverter = NameConverters.ToKebabCase,
            }
        }
    };
}