using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

/// <summary>
/// A collection of utility methods for creating entities in the database
/// </summary>
public static class CreateCommands
{
    #region Regular
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity</returns>
    public static int Insert<TEntity>(this IDbConnection connection, TEntity entity)
        => connection.Insert<TEntity, int>(entity);
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity</returns>
    public static int Insert<TEntity>(this IDbTransaction transaction, TEntity entity)
        => transaction.Insert<TEntity, int>(entity);
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/></returns>
    public static TKey Insert<TEntity, TKey>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateInsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        
        return connection.QuerySingle<TKey>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/></returns>
    public static TKey Insert<TEntity, TKey>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateInsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        
        return transaction.QuerySingle<TKey>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">An array of entities to insert</param>
    /// <param name="batchSize">The batch size for bulk insertion</param>
    /// <returns>The number of inserted entries</returns>
    public static int BulkInsert<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateInsertSql(entries, builderData, start, end, false), entities, batchSize);
    }
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">An array of entities to insert</param>
    /// <param name="batchSize">The batch size for bulk insertion</param>
    /// <returns>The number of inserted entries</returns>
    public static int BulkInsert<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateInsertSql(entries, builderData, start, end, false), entities, batchSize);
    }
    
    #endregion
    
    #region Async
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity</returns>
    public static Task<int> InsertAsync<TEntity>(
        this IDbConnection connection,
        TEntity entity)
        => connection.InsertAsync<TEntity, int>(entity);
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity</returns>
    public static Task<int> InsertAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity entity)
        => transaction.InsertAsync<TEntity, int>(entity);
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/></returns>
    public static Task<TKey> InsertAsync<TEntity, TKey>(
        this IDbConnection connection,
        TEntity entity)
    {
        CommandComponents components = GenerateInsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        
        return connection.QuerySingleAsync<TKey>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <typeparam name="TKey">The type of the key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity to insert</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/></returns>
    public static Task<TKey> InsertAsync<TEntity, TKey>(
        this IDbTransaction transaction,
        TEntity entity)
    {
        CommandComponents components = GenerateInsertSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        
        return transaction.QuerySingleAsync<TKey>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">An array of entities to insert</param>
    /// <param name="batchSize">The batch size for bulk insertion</param>
    /// <returns>The number of inserted entries</returns>
    public static Task<int> BulkInsertAsync<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulkAsync(
            (entries, start, end) => GenerateInsertSql(entries, builderData, start, end, false), entities, batchSize);
    }
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">An array of entities to insert</param>
    /// <param name="batchSize">The batch size for bulk insertion</param>
    /// <returns>The number of inserted entries</returns>
    public static Task<int> BulkInsertAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateInsertSql(entries, builderData, start, end, false), entities, batchSize);
    }
    
    #endregion
    private static CommandComponents GenerateInsertSql<TEntities>(
        TEntities[] entities,
        CommandBuilderData data,
        int enumerationStartIndex = 0,
        int enumerationEnd = 1,
        bool returningKey = true)
    {
        StringBuilder valuesBuilder = new();
        DynamicParameters values = new();
        
        for(int i=enumerationStartIndex; i<enumerationEnd; i++)
        {
            TEntities entity = entities[i];

            valuesBuilder
                .AppendWithSeparator("(", ',')
                .AppendInsertParameters(
                    data,
                    ref values,
                    i,
                    entity)
                .Append(")");
        }
        
        string generatedQuery = BuildInsertCommand(valuesBuilder, data, returningKey);
    
        return new(generatedQuery, values);
    }

    private static string BuildInsertCommand(
        StringBuilder valuesBuilder,
        CommandBuilderData data,
        bool returning)
    {
        StringBuilder builder = new("INSERT INTO ");
        builder
            .Append(data.DbIdentifier)
            .Append(" (")
            .Append(data.ColumnsString)
            .Append(")");
        
        if(data.Options.Dialect != SqlDialect.SqlServer)
        {
            builder
                .Append(" VALUES ")
                .Append(valuesBuilder);
        }

        if (returning)
        {
            builder.Append(data.Options.Dialect switch
            {
                SqlDialect.PostgreSql => " RETURNING ",
                SqlDialect.SqlServer => " OUTPUT ",
                SqlDialect.MySql or SqlDialect.MariaDb or SqlDialect.Sqlite => ";SELECT ",
                _ => ""
            });
            
            for (int i = 0; i < data.KeyProperties.Length; i++)
            {
                switch(data.Options.Dialect)
                {
                    case SqlDialect.PostgreSql:
                        builder.Append('"').Append(data.KeyProperties[i].DbName).Append('"');
                        break;
                    case SqlDialect.SqlServer:
                        builder.Append("INSERTED.").Append(data.KeyProperties[i].DbName);
                        break;
                    case SqlDialect.MySql or SqlDialect.MariaDb or SqlDialect.Sqlite:
                        if (data.KeyProperties[i].IsDbGenerated)
                        {
                            builder.Append(data.Options.Dialect == SqlDialect.Sqlite ?
                                "LAST_INSERT_ROWID()":
                                "LAST_INSERT_ID()");
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
                    builder.Append(",");
                }
            }
        }
        
        if(data.Options.Dialect == SqlDialect.SqlServer)
        {
            builder
                .Append(" VALUES ")
                .Append(valuesBuilder);
        }
                    
        return builder.ToString();
    }
    
    private static StringBuilder AppendInsertParameters<TEntity>(
        this StringBuilder sb,
        CommandBuilderData builderData,
        ref DynamicParameters parameters,
        int index,
        TEntity entity)
    {
        bool firstSkipPending = true;
        foreach (var property in builderData.Properties.Where(p => !p.IsDbGenerated))
        {
            string key = string.Concat("@", property.AssemblyName, index);
            sb.AppendWithSeparator(key, ',', firstSkipPending);
            firstSkipPending = false;
            parameters.Add(key, property.Getter(entity!));
        }

        return sb;
    }
    
}