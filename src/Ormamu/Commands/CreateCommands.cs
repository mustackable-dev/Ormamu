using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

/// <summary>
/// This is the main class for Ormamu. All extensions for IDbConnection and IDbTransaction are housed here.
/// </summary>
public static class CreateCommands
{
    #region Regular
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity.</returns>
    public static int Insert<TValue>(this IDbConnection connection, TValue entity)
        => connection.QuerySingle<int>(GenerateInsertSql<TValue>(), entity);
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity.</returns>
    public static int Insert<TValue>(this IDbTransaction transaction, TValue entity)
        => transaction.QuerySingle<int>(GenerateInsertSql<TValue>(), entity);
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/>.</returns>
    public static TKey Insert<TKey, TValue>(this IDbConnection connection, TValue entity)
        => connection.QuerySingle<TKey>(GenerateInsertSql<TValue>(), entity)!;
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/>.</returns>
    public static TKey Insert<TKey, TValue>(
        this IDbTransaction transaction,
        TValue entity)
        => transaction.QuerySingle<TKey>(GenerateInsertSql<TValue>(), entity)!;
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">An array of entities to insert.</param>
    /// <param name="batchSize">The batch size for bulk insertion.</param>
    /// <returns>The number of inserted entries.</returns>
    public static int BulkInsert<TValue>(
        this IDbConnection connection,
        TValue[] entities,
        int batchSize = 100)
    {
        int createdEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkInsertComponents components =
                GenerateBulkInsertSql(
                    entities.AsSpan(createdEntries, currentBatchSize),
                    data);
            createdEntries += connection.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return createdEntries;
    }
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">An array of entities to insert.</param>
    /// <param name="batchSize">The batch size for bulk insertion.</param>
    /// <returns>The number of inserted entries.</returns>
    public static int BulkInsert<TValue>(
        this IDbTransaction transaction,
        TValue[] entities,
        int batchSize = 100)
    {
        int createdEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkInsertComponents components =
                GenerateBulkInsertSql(
                    entities.AsSpan(createdEntries, currentBatchSize),
                    data);
            createdEntries += transaction.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return createdEntries;
    }
    
    #endregion
    
    #region Async
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity.</returns>
    public static Task<int> InsertAsync<TValue>(
        this IDbConnection connection,
        TValue entity)
        => connection.QuerySingleAsync<int>(GenerateInsertSql<TValue>(), entity);
    
    /// <summary>
    /// Inserts an entity with a key of type <see cref="int"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The key (of type <see cref="int"/>) of the inserted entity.</returns>
    public static Task<int> InsertAsync<TValue>(
        this IDbTransaction transaction,
        TValue entity)
        => transaction.QuerySingleAsync<int>(GenerateInsertSql<TValue>(), entity)!;
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/>.</returns>
    public static Task<TKey> InsertAsync<TKey, TValue>(
        this IDbConnection connection,
        TValue entity)
        => connection.QuerySingleAsync<TKey>(GenerateInsertSql<TValue>(), entity);
    
    /// <summary>
    /// Inserts an entity with a generic key of type <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity to insert.</param>
    /// <returns>The inserted entity's key of type <typeparamref name="TKey"/>.</returns>
    public static Task<TKey> InsertAsync<TKey, TValue>(
        this IDbTransaction transaction,
        TValue entity)
        => transaction.QuerySingleAsync<TKey>(GenerateInsertSql<TValue>(), entity)!;
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">An array of entities to insert.</param>
    /// <param name="batchSize">The batch size for bulk insertion.</param>
    /// <returns>The number of inserted entries.</returns>
    public static async Task<int> BulkInsertAsync<TValue>(
        this IDbConnection connection,
        TValue[] entities,
        int batchSize = 100)
    {
        int createdEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkInsertComponents components =
                GenerateBulkInsertSql(
                    entities.AsSpan(createdEntries, currentBatchSize),
                    data);
            createdEntries += await connection.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return createdEntries;
    }
    
    /// <summary>
    /// Inserts an array of entities via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">An array of entities to insert.</param>
    /// <param name="batchSize">The batch size for bulk insertion.</param>
    /// <returns>The number of inserted entries.</returns>
    public static async Task<int> BulkInsertAsync<TValue>(
        this IDbTransaction transaction,
        TValue[] entities,
        int batchSize = 100)
    {
        int createdEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkInsertComponents components =
                GenerateBulkInsertSql(
                    entities.AsSpan(createdEntries, currentBatchSize),
                    data);
            createdEntries += await transaction.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return createdEntries;
    }
    
    #endregion
    
    private static string GenerateInsertSql<TValue>()
    {
        StringBuilder valuesBuilder = new();
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        valuesBuilder.Append("(").AppendProperties(data.Properties, AppendType.Assembly).Append(")");

        return BuildInsertCommand(valuesBuilder, data, true);
    }
    

    private sealed record BulkInsertComponents(string Command, DynamicParameters Parameters);
    private static BulkInsertComponents GenerateBulkInsertSql<TValue>(
        Span<TValue> entities,
        CommandBuilderData data)
    {
        StringBuilder valuesBuilder = new();
        DynamicParameters values = new();
        
        for(int i=0; i<entities.Length; i++)
        {
            TValue entity = entities[i];

            valuesBuilder
                .AppendWithSeparator("(", ',')
                .AppendInsertParameters(
                    data,
                    ref values,
                    i,
                    entity)
                .Append(")");
        }
        
        string generatedQuery = BuildInsertCommand(valuesBuilder, data, false);
    
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
        
        switch (data.Options.Dialect)
        {
            case SqlDialect.PostgreSql:
                builder
                    .Append(" VALUES ")
                    .Append(valuesBuilder);
                
                if (returning)
                {
                    builder.Append(" RETURNING ");
                    if (data.CompositeKeyData is null)
                    {
                        builder.Append(string.Concat('"', data.PrimaryKey.DbName, '"'));
                    }
                    else
                    {
                        for (int i = 0; i < data.CompositeKeyData.Properties.Length; i++)
                        {
                            builder.Append(string.Concat('"', data.CompositeKeyData.Properties[i].DbName, '"'));
                            if (i < data.CompositeKeyData.Properties.Length - 1)
                            {
                                builder.Append(",");
                            }
                        }
                    }
                }
                break;
            
            case SqlDialect.SqlServer:
                if (returning)
                {
                    builder.Append(" OUTPUT ");
                    
                    if (data.CompositeKeyData is null)
                    {
                        builder.Append("INSERTED.").Append(data.PrimaryKey.DbName);
                    }
                    else
                    {
                        for (int i = 0; i < data.CompositeKeyData.Properties.Length; i++)
                        {
                            builder.Append("INSERTED.").Append(data.CompositeKeyData.Properties[i].DbName);
                            if (i < data.CompositeKeyData.Properties.Length - 1)
                            {
                                builder.Append(",");
                            }
                        }
                    }
                }

                builder.Append(" VALUES ").Append(valuesBuilder);
                break;
            
            case SqlDialect.MySql:
            case SqlDialect.MariaDb:
                builder
                    .Append(" VALUES ")
                    .Append(valuesBuilder);
                
                if (returning)
                {
                    builder.Append(";SELECT ");
                    if (data.PrimaryKey.IsDbGenerated)
                    {
                        builder.Append("LAST_INSERT_ID()");
                    }
                    else
                    {
                        if (data.CompositeKeyData is null)
                        {
                            builder.Append('@').Append(data.PrimaryKey.AssemblyName);
                        }
                        else
                        {
                            for (int i = 0; i < data.CompositeKeyData.Properties.Length; i++)
                            {
                                builder
                                    .Append("@")
                                    .Append(data.CompositeKeyData.Properties[i].AssemblyName)
                                    .Append(" as ")
                                    .Append(data.CompositeKeyData.Properties[i].DbName);
                                if (i < data.CompositeKeyData.Properties.Length - 1)
                                {
                                    builder.Append(",");
                                }
                            }
                        }
                    }
                }
                break;
            
            case SqlDialect.Sqlite:
                builder
                    .Append(" VALUES ")
                    .Append(valuesBuilder);
                
                if (returning)
                {
                    builder.Append(";SELECT ");
                    if (data.PrimaryKey.IsDbGenerated)
                    {
                        builder.Append("LAST_INSERT_ROWID()");
                    }
                    else
                    {
                        if (data.CompositeKeyData is null)
                        {
                            builder.Append('@').Append(data.PrimaryKey.AssemblyName);
                        }
                        else
                        {
                            for (int i = 0; i < data.CompositeKeyData.Properties.Length; i++)
                            {
                                builder
                                    .Append("@")
                                    .Append(data.CompositeKeyData.Properties[i].AssemblyName)
                                    .Append(" as ")
                                    .Append(data.CompositeKeyData.Properties[i].DbName);
                                if (i < data.CompositeKeyData.Properties.Length - 1)
                                {
                                    builder.Append(",");
                                }
                            }
                        }
                    }
                }
                break;
        }
                    
        return builder.ToString();
    }
    
    private static StringBuilder AppendInsertParameters<TValue>(
        this StringBuilder sb,
        CommandBuilderData builderData,
        ref DynamicParameters parameters,
        int index,
        TValue entity)
    {
        for (var i = 0; i < builderData.Properties.Length; i++)
        {
            var property = builderData.Properties[i];
            if (property.IsDbGenerated) continue;

            string key = string.Concat("@", property.AssemblyName, index);
            sb.AppendWithSeparator(key, ',', i == 0);

            parameters.Add(key, property.Getter(entity!));
        }
        return sb;
    }
    
}