using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

/// <summary>
/// A collection of utility methods for updating entities in the database
/// </summary>
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
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TValue)));
        return connection.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TValue"/> using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static int Update<TValue>(this IDbTransaction transaction, TValue entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TValue)));
        return transaction.Execute(components.Command, components.Parameters);
    }

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
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TValue));
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
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
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TValue));
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
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
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TValue)));
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TValue"/> using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static Task<int> UpdateAsync<TValue>(this IDbTransaction transaction, TValue entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TValue)));
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TValue"/> in batches using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static Task<int> BulkUpdateAsync<TValue>(
        this IDbConnection connection,
        TValue[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TValue));
        return connection.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TValue"/> in batches using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static Task<int> BulkUpdateAsync<TValue>(
        this IDbTransaction transaction,
        TValue[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TValue));
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }
     
    #endregion
    private static CommandComponents GenerateUpdateSql<TValue>(
        TValue[] entities,
        CommandBuilderData data,
        int enumerationStartIndex = 0,
        int enumerationEnd = 1)
    {
        StringBuilder commandBuilder = new();
        
        DynamicParameters values = new(entities.Length);
        
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        for(int i=enumerationStartIndex; i<enumerationEnd; i++)
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
        
            if (data.KeyProperties.Length == 1)
            {
                commandBuilder.AppendEquality(data.KeyProperties[0], true, propertyWrapper, i);
            }
            else
            {
                for (int j = 0; j < data.KeyProperties.Length; j++)
                {
                    commandBuilder.AppendEquality(data.KeyProperties[j], true, propertyWrapper, i);
                    if (j < data.KeyProperties.Length - 1)
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