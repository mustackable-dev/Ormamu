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
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static int Update<TEntity>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return connection.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static int Update<TEntity>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return transaction.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static int BulkUpdate<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static int BulkUpdate<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }
     
    #endregion
    
    #region Async

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static Task<int> UpdateAsync<TEntity>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entity">The entity with updated values.</param>
    /// <returns>The number of affected rows (typically 1).</returns>
    public static Task<int> UpdateAsync<TEntity>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static Task<int> BulkUpdateAsync<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="entities">The array of entities with updated values.</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100.</param>
    /// <returns>The total number of affected rows.</returns>
    public static Task<int> BulkUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }
     
    #endregion
    
    #region PartialRegular
    public static int PartialUpdate<TEntity>(
        this IDbConnection connection,
        TEntity entity,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials);
        
        return connection.Execute(components.Command, components.Parameters);
    }
    
    public static int PartialUpdate<TEntity>(
        this IDbTransaction transaction,
        TEntity entity,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials);
        
        return transaction.Execute(components.Command, components.Parameters);
    }
    
    public static int PartialUpdate<TEntity>(
        this IDbConnection connection,
        int entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => connection.PartialUpdate<int, TEntity>(entityKey, propertySettersFactory);

    public static int PartialUpdate<TEntity>(
        this IDbTransaction transaction,
        int entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => transaction.PartialUpdate<int, TEntity>(entityKey, propertySettersFactory);
    
    public static int PartialUpdate<TKey, TEntity>(
        this IDbConnection connection,
        TKey entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [entityKey],
            builderData,
            partials: partials);
        return connection.Execute(components.Command, components.Parameters);
    }

    public static int PartialUpdate<TKey, TEntity>(
        this IDbTransaction transaction,
        TKey entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [entityKey],
            builderData,
            partials: partials);
        return transaction.Execute(components.Command, components.Parameters);
    }
    public static int BulkPartialUpdate<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), entities, batchSize);
    }
    
    public static int BulkPartialUpdate<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), entities, batchSize);
    }
    
    public static int BulkPartialUpdate<TEntity>(
        this IDbConnection connection,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => connection.BulkPartialUpdate<int, TEntity>(keys, propertySettersFactory, batchSize);

    public static int BulkPartialUpdate<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => transaction.BulkPartialUpdate<int, TEntity>(keys, propertySettersFactory, batchSize);
    
    public static int BulkPartialUpdate<TKey, TEntity>(
        this IDbConnection connection,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), keys, batchSize);
    }

    public static int BulkPartialUpdate<TKey, TEntity>(
        this IDbTransaction transaction,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), keys, batchSize);
    }
    
    #endregion
    
    #region PartialAsync
    public static Task<int> PartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        TEntity entity,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials);
        
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }
    
    public static Task<int> PartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity entity,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials);
        
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
    
    public static Task<int> PartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        int entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => connection.PartialUpdateAsync<int, TEntity>(entityKey, propertySettersFactory);

    public static Task<int> PartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        int entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => transaction.PartialUpdateAsync<int, TEntity>(entityKey, propertySettersFactory);
    
    public static Task<int> PartialUpdateAsync<TKey, TEntity>(
        this IDbConnection connection,
        TKey entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [entityKey],
            builderData,
            partials: partials);
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }

    public static Task<int> PartialUpdateAsync<TKey, TEntity>(
        this IDbTransaction transaction,
        TKey entityKey,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [entityKey],
            builderData,
            partials: partials);
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return connection.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), entities, batchSize);
    }
    
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), entities, batchSize);
    }
    
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => connection.BulkPartialUpdateAsync<int, TEntity>(keys, propertySettersFactory, batchSize);

    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => transaction.BulkPartialUpdateAsync<int, TEntity>(keys, propertySettersFactory, batchSize);
    
    public static Task<int> BulkPartialUpdateAsync<TKey, TEntity>(
        this IDbConnection connection,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return connection.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), keys, batchSize);
    }

    public static Task<int> BulkPartialUpdateAsync<TKey, TEntity>(
        this IDbTransaction transaction,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end, partials), keys, batchSize);
    }
    #endregion
    private static CommandComponents GenerateUpdateSql<TValue>(
        TValue[] values,
        CommandBuilderData data,
        int enumerationStartIndex = 0,
        int enumerationEnd = 1,
        PartialUpdateComponents? partials = null)
    {
        StringBuilder commandBuilder = new();
        DynamicParameters parameters = new(partials is null ? values.Length : partials.Updates.Length);
        
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        for(int i=enumerationStartIndex; i<enumerationEnd; i++)
        {
            commandBuilder.Append("UPDATE ").Append(data.DbIdentifier).Append(" SET ");

            if (partials is null)
            {
                commandBuilder.AppendUpdateParameters(
                    data.Properties,
                    ref parameters,
                    i,
                    values[i],
                    propertyWrapper);
            }
            else
            {
                if (partials.Copy)
                {
                    
                    commandBuilder.AppendUpdateParameters(
                        partials.Updates.Select(x=>x.Property),
                        ref parameters,
                        i,
                        values[i],
                        propertyWrapper);
                }
                else
                {
                    commandBuilder.AppendUpdateParameters(
                        ref parameters,
                        i,
                        values[i],
                        partials.Updates,
                        propertyWrapper);
                }
            }

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
    
        return new(commandBuilder.ToString(), parameters);
    }
    
    private static void AppendUpdateParameters<TEntity>(
        this StringBuilder sb,
        IEnumerable<PropertyMapping> properties,
        ref DynamicParameters parameters,
        int index,
        TEntity entity,
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
    
    private static void AppendUpdateParameters<TValue>(
        this StringBuilder sb,
        ref DynamicParameters parameters,
        int index,
        TValue keyValue,
        PartialUpdates[] partials,
        char propertyWrapper = '\0',
        bool hasCompositeKey = false)
    {
        bool skipFirst = true;
        foreach (PartialUpdates update in partials)
        {
            string key = string.Concat("@", update.Property.AssemblyName, index);

            if (!update.Property.IsDbGenerated)
            {
                if (update.KeySetter)
                {
                    parameters.Add(
                        key,
                        hasCompositeKey && keyValue is not null ?
                            update.Property.CompositeKeyGetter!(keyValue) :
                            keyValue);
                }
                else
                {
                    parameters.Add(key, update.Value);
                }
            }

            if (!update.Property.IsKey)
            {
                sb.AppendWithSeparatorAndWrapper(update.Property.DbName, propertyWrapper, ',', skipFirst);
                sb.Append("=").Append(key);
                skipFirst = false;
            }
        }
    }
}