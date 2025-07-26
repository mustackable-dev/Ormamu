using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

public static class UpdateCommands
{
    
    #region Regular

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TValue"/> using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static int Update<TValue>(this IDbConnection connection, TValue entity)
        => connection.Execute(GenerateUpdateSql<TValue>(), entity);

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TValue"/> using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static int Update<TValue>(this IDbTransaction transaction, TValue entity)
        => transaction.Execute(GenerateUpdateSql<TValue>(), entity);

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TValue"/> in batches using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static int BulkUpdate<TValue>(
        this IDbConnection connection,
        TValue[] entities,
        int batchSize = 100)
    {
        int updatedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkUpdateComponents components =
                GenerateBulkUpdateSql(
                    entities.AsSpan(updatedEntries, currentBatchSize),
                    data);
            updatedEntries += connection.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return updatedEntries;
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TValue"/> in batches using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static int BulkUpdate<TValue>(
        this IDbTransaction transaction,
        TValue[] entities,
        int batchSize = 100)
    {
        int updatedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkUpdateComponents components =
                GenerateBulkUpdateSql(
                    entities.AsSpan(updatedEntries, currentBatchSize),
                    data);
            updatedEntries += transaction.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return updatedEntries;
    }
     
    #endregion
    
    #region Async

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TValue"/> using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static Task<int> UpdateAsync<TValue>(this IDbConnection connection, TValue entity)
        => connection.ExecuteAsync(GenerateUpdateSql<TValue>(), entity);

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TValue"/> using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static Task<int> UpdateAsync<TValue>(this IDbTransaction transaction, TValue entity)
        => transaction.ExecuteAsync(GenerateUpdateSql<TValue>(), entity);

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TValue"/> in batches using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static async Task<int> BulkUpdateAsync<TValue>(
        this IDbConnection connection,
        TValue[] entities,
        int batchSize = 100)
    {
        int updatedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkUpdateComponents components =
                GenerateBulkUpdateSql(
                    entities.AsSpan(updatedEntries, currentBatchSize),
                    data);
            updatedEntries += await connection.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return updatedEntries;
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TValue"/> in batches using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static async Task<int> BulkUpdateAsync<TValue>(
        this IDbTransaction transaction,
        TValue[] entities,
        int batchSize = 100)
    {
        int updatedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = entities.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkUpdateComponents components =
                GenerateBulkUpdateSql(
                    entities.AsSpan(updatedEntries, currentBatchSize),
                    data);
            updatedEntries += await transaction.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return updatedEntries;
    }
     
    #endregion
    
    private static string GenerateUpdateSql<TValue>()
    {
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };

        StringBuilder commandBuilder = new StringBuilder("UPDATE ").Append(data.DbIdentifier)
            .Append(" SET ").AppendProperties(data.Properties, AppendType.Equality, propertyWrapper, true)
            .Append(" WHERE ");
        
        if (data.CompositeKeyData is null)
        {
            commandBuilder.AppendEquality(data.PrimaryKey, true, propertyWrapper);
        }
        else
        {
            for (int i = 0; i < data.CompositeKeyData.Properties.Length; i++)
            {
                commandBuilder.AppendEquality(data.CompositeKeyData.Properties[i], true, propertyWrapper);
                if (i < data.CompositeKeyData.Properties.Length - 1)
                {
                    commandBuilder.Append(" AND ");
                }
            }
        }
        
        return commandBuilder.ToString();
    }
    
    private sealed record BulkUpdateComponents(string Command, DynamicParameters Parameters);
    
    private static BulkUpdateComponents GenerateBulkUpdateSql<TValue>(
        Span<TValue> entities,
        CommandBuilderData data)
    {
        StringBuilder commandBuilder = new();
        
        DynamicParameters values = new(entities.Length);
        
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        for(int i=0; i<entities.Length; i++)
        {
            commandBuilder.Append("UPDATE ").Append(data.DbIdentifier).Append(" SET ");
            
            commandBuilder.AppendUpdateParameters(
                data.Properties,
                ref values,
                i,
                entities[i],
                propertyWrapper);

            commandBuilder
                .Append(" WHERE ");
        
            if (data.CompositeKeyData is null)
            {
                commandBuilder.AppendEquality(data.PrimaryKey, true, propertyWrapper, i);
            }
            else
            {
                for (int j = 0; j < data.CompositeKeyData.Properties.Length; j++)
                {
                    commandBuilder.AppendEquality(data.CompositeKeyData.Properties[j], true, propertyWrapper, i);
                    if (j < data.CompositeKeyData.Properties.Length - 1)
                    {
                        commandBuilder.Append(" AND ");
                    }
                }
            }
        
            commandBuilder.Append(";");
        }
    
        return new(commandBuilder.ToString(), values);
    }
    
    private static void AppendUpdateParameters<TValue>(
        this StringBuilder sb,
        PropertyMapping[] properties,
        ref DynamicParameters parameters,
        int index,
        TValue entity,
        char propertyWrapper = '\0')
    {
        bool skipFirst = true;
        foreach (PropertyMapping property in properties)
        {
            string key = string.Concat("@", property.AssemblyName, index);
            
            if(!property.IsDbGenerated) parameters.Add(key, property.Getter(entity!));

            if (!property.IsKey)
            {
                sb.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', skipFirst);
                sb.Append("=").Append(key);
                skipFirst = false;
            }
        }
    }
}