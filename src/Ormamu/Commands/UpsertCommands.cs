using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;
using Ormamu.Exceptions;

namespace Ormamu;

/// <summary>
/// A collection of utility methods for upserting entities in the database
/// </summary>
/// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
public static class UpsertCommands
{
    #region Regular

    /// <summary>
    /// Upserts an entity with a key of type <see cref="int"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The key (of type <see cref="int"/>) of the upserted entity</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static int Upsert<TEntity>(this IDbConnection connection, TEntity entity)
        => connection.Upsert<TEntity, int>(entity);

    /// <summary>
    /// Upserts an entity with a key of type <see cref="int"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The key (of type <see cref="int"/>) of the upserted entity</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static int Upsert<TEntity>(this IDbTransaction transaction, TEntity entity)
        => transaction.Upsert<TEntity, int>(entity);

    /// <summary>
    /// Upserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The upserted entity's key of type <typeparamref name="TKey"/></returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static TKey Upsert<TEntity, TKey>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateUpsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));

        return connection.QuerySingle<TKey>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Upserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The upserted entity's key of type <typeparamref name="TKey"/></returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static TKey Upsert<TEntity, TKey>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateUpsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));

        return transaction.QuerySingle<TKey>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Upserts an array of entities via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">An array of entities to upsert</param>
    /// <param name="batchSize">The batch size for bulk upsertion</param>
    /// <returns>The number of upserted entries</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static int BulkUpsert<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateUpsertSql(entries, builderData, start, end, false),
            entities,
            batchSize,
            true);
    }

    /// <summary>
    /// Upserts an array of entities via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">An array of entities to upsert</param>
    /// <param name="batchSize">The batch size for bulk upsertion</param>
    /// <returns>The number of upserted entries</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static int BulkUpsert<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateUpsertSql(entries, builderData, start, end, false),
            entities,
            batchSize,
            true);
    }

    #endregion

    #region Async

    /// <summary>
    /// Upserts an entity with a key of type <see cref="int"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The key (of type <see cref="int"/>) of the upserted entity</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static Task<int> UpsertAsync<TEntity>(
        this IDbConnection connection,
        TEntity entity)
        => connection.UpsertAsync<TEntity, int>(entity);

    /// <summary>
    /// Upserts an entity with a key of type <see cref="int"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The key (of type <see cref="int"/>) of the upserted entity</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static Task<int> UpsertAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity entity)
        => transaction.UpsertAsync<TEntity, int>(entity);

    /// <summary>
    /// Upserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The upserted entity's key of type <typeparamref name="TKey"/></returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static Task<TKey> UpsertAsync<TEntity, TKey>(
        this IDbConnection connection,
        TEntity entity)
    {
        CommandComponents components = GenerateUpsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));

        return connection.QuerySingleAsync<TKey>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Upserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to upsert</param>
    /// <returns>The upserted entity's key of type <typeparamref name="TKey"/></returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static Task<TKey> UpsertAsync<TEntity, TKey>(
        this IDbTransaction transaction,
        TEntity entity)
    {
        CommandComponents components = GenerateUpsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));

        return transaction.QuerySingleAsync<TKey>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Upserts an array of entities via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">An array of entities to upsert</param>
    /// <param name="batchSize">The batch size for bulk upsertion</param>
    /// <returns>The number of upserted entries</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static Task<int> BulkUpsertAsync<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpsertSql(entries, builderData, start, end, false),
            entities,
            batchSize,
            true);
    }

    /// <summary>
    /// Upserts an array of entities via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">An array of entities to upsert</param>
    /// <param name="batchSize">The batch size for bulk upsertion</param>
    /// <returns>The number of upserted entries</returns>
    /// <remarks>Experimental feature, SQL Server is not supported yet.</remarks>
    public static Task<int> BulkUpsertAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpsertSql(entries, builderData, start, end, false),
            entities,
            batchSize,
            true);
    }

    #endregion

    private static CommandComponents GenerateUpsertSql<TEntities>(
        TEntities[] entities,
        CommandBuilderData data,
        int enumerationStartIndex = 0,
        int enumerationEnd = 1,
        bool returningKey = true)
    {
        DynamicParameters values = new();

        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _ => '\0'
        };

        StringBuilder builder = new();


        for (int i = enumerationStartIndex; i < enumerationEnd; i++)
        {
            TEntities entity = entities[i];

            List<PropertyMapping> autogenerateExcludeList = GetAutogenerateExcludeList(data, entity);

            StringBuilder valuesBuilder = new();
            valuesBuilder
                .AppendWithSeparator("(", ',')
                .AppendUpsertParameters(
                    data,
                    ref values,
                    i,
                    entity,
                    autogenerateExcludeList)
                .Append(")");

            builder
                .AppendUpsertCommand(valuesBuilder, data, propertyWrapper, autogenerateExcludeList)
                .AppendConflictResolution(data, propertyWrapper, autogenerateExcludeList);

            if (returningKey)
                builder.AppendReturningPart(data, autogenerateExcludeList);

            builder.Append(';');
        }

        return new(builder.ToString(), values);
    }

    private static List<PropertyMapping> GetAutogenerateExcludeList<TEntity>(CommandBuilderData data, TEntity entity)
    {
        List<PropertyMapping> result = new();
        foreach (PropertyMapping property in data.Properties.Where(x => x.IsDbGenerated))
        {
            if (!property.IsKey)
            {
                result.Add(property);
                continue;
            }

            object value = property.Getter(entity!);
            if ((!property.Type.IsValueType && value is null) ||
                (property.Type.IsValueType && value.Equals(Activator.CreateInstance(property.Type))))
                result.Add(property);
        }

        return result;
    }

    private static StringBuilder AppendUpsertCommand(
        this StringBuilder builder,
        StringBuilder valuesBuilder,
        CommandBuilderData data,
        char propertyWrapper,
        List<PropertyMapping> autogenerateExcludeList)
    {
        builder.Append("INSERT INTO ");
        builder
            .Append(data.DbIdentifier)
            .Append(" (");

        bool firstSkipPending = true;
        foreach (PropertyMapping property in data.Properties.Except(autogenerateExcludeList))
        {
            builder.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', firstSkipPending);
            firstSkipPending = false;
        }

        builder
            .Append(")");

        if (data.Options.Dialect != SqlDialect.SqlServer)
        {
            builder
                .Append(" VALUES ")
                .Append(valuesBuilder);
        }

        return builder;
    }

    private static StringBuilder AppendUpsertParameters<TEntity>(
        this StringBuilder sb,
        CommandBuilderData builderData,
        ref DynamicParameters parameters,
        int index,
        TEntity entity,
        List<PropertyMapping> keyExcludeList)
    {
        bool firstSkipPending = true;
        foreach (var property in builderData.Properties.Except(keyExcludeList).Where(x => !x.IsDbGenerated || x.IsKey))
        {
            string key = string.Concat("@", property.AssemblyName, index);
            sb.AppendWithSeparator(key, ',', firstSkipPending);
            firstSkipPending = false;
            parameters.Add(key, property.Getter(entity!));
        }

        return sb;
    }

    private static void AppendConflictResolution(
        this StringBuilder builder,
        CommandBuilderData data,
        char propertyWrapper,
        List<PropertyMapping> autogenerateExcludeList)
    {
        bool firstSkipPending = true;
        switch (data.Options.Dialect)
        {
            case SqlDialect.PostgreSql:
            case SqlDialect.Sqlite:
                builder.Append(" ON CONFLICT (");

                foreach (PropertyMapping property in data.KeyProperties)
                {
                    // SQLite cannot natively have an autoincrementing column as part of its 
                    // composite key, so we need to exclude autoincrementing columns from
                    // the ON CONFLICT column collection, otherwise you will get an error.
                    
                    // This creates another problem, though. The ON CONFLICT clause will
                    // ignore the autoincrementing column and just trigger on matches in 
                    // the non-autoincrementing parts of the composite key, which may
                    // create updates, instead of inserts. This seems to be a genuine 
                    // limitation of Sqlite that cannot be bypassed.
                    
                    // if (data.Options.Dialect == SqlDialect.Sqlite &&
                    //     property.CompositeKeyGetter is not null &&
                    //     property is { IsKey: true, IsDbGenerated: true })
                    //     continue;
                    
                    builder.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', firstSkipPending);
                    firstSkipPending = false;
                }

                builder.Append(") DO UPDATE SET ");
                builder.AppendConflictSetParameters(
                    data.Properties,
                    (sb, property) => sb
                        .Append("EXCLUDED.")
                        .AppendWithWrapper(property.DbName, propertyWrapper),
                    autogenerateExcludeList,
                    propertyWrapper);
                break;

            case SqlDialect.MariaDb:
            case SqlDialect.MySql:
                builder.Append(" ON DUPLICATE KEY UPDATE ");
                builder.AppendConflictSetParameters(
                    data.Properties,
                    (sb, property) =>
                    {
                        sb
                            .Append(
                                property is { IsKey: true, IsDbGenerated: true } &&
                                !autogenerateExcludeList.Contains(property)
                                    ? "LAST_INSERT_ID("
                                    : "VALUES(")
                            .AppendWithWrapper(property.DbName, propertyWrapper)
                            .Append(')');
                    },
                    autogenerateExcludeList,
                    propertyWrapper);
                break;
        }
    }

    private static void AppendConflictSetParameters(
        this StringBuilder sb,
        IEnumerable<PropertyMapping> properties,
        Action<StringBuilder, PropertyMapping> valueGenerator,
        List<PropertyMapping> autogenerateExcludeList,
        char propertyWrapper = '\0')
    {
        bool skipFirst = true;
        int initialLength = sb.Length;
        foreach (PropertyMapping property in properties.Except(autogenerateExcludeList))
        {
            sb.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', skipFirst);
            sb.Append('=');
            valueGenerator(sb, property);
            skipFirst = false;
        }

        if (initialLength == sb.Length)
            throw new CommandBuilderException(CommandBuilderExceptionType.InvalidUpdatePayload);
    }

    private static void AppendReturningPart(
        this StringBuilder builder,
        CommandBuilderData data,
        List<PropertyMapping> autogenerateExcludeList)
    {
        builder.Append(
            data.Options.Dialect switch
            {
                SqlDialect.PostgreSql => " RETURNING ",
                SqlDialect.SqlServer => " OUTPUT ",
                SqlDialect.MySql or SqlDialect.MariaDb or SqlDialect.Sqlite => ";SELECT ",
                _ => ""
            });

        for (int i = 0; i < data.KeyProperties.Length; i++)
        {
            switch (data.Options.Dialect)
            {
                case SqlDialect.PostgreSql:
                    builder.Append('"').Append(data.KeyProperties[i].DbName).Append('"');
                    break;
                case SqlDialect.SqlServer:
                    builder.Append("INSERTED.").Append(data.KeyProperties[i].DbName);
                    break;
                case SqlDialect.MySql or SqlDialect.MariaDb or SqlDialect.Sqlite:
                    if (data.KeyProperties[i].IsDbGenerated &&
                        autogenerateExcludeList.Exists(x => x.AssemblyName == data.KeyProperties[i].AssemblyName))
                    {
                        builder.Append(
                            data.Options.Dialect == SqlDialect.Sqlite ? "LAST_INSERT_ROWID()" : "LAST_INSERT_ID()");
                    }
                    else
                    {
                        builder.Append("@").Append(data.KeyProperties[i].AssemblyName).Append(0);
                    }

                    builder.Append(" as ").Append(data.KeyProperties[i].AssemblyName);
                    break;
            }

            if (i < data.KeyProperties.Length - 1)
            {
                builder.Append(',');
            }
        }
    }
}