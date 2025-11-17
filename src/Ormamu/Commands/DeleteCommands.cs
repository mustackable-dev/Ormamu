using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;
using Ormamu.Exceptions;

namespace Ormamu;

/// <summary>
/// A collection of utility methods for deleting entities from the database
/// </summary>
public static class DeleteCommands
{
    #region Regular
    
    /// <summary>
    /// Deletes the specified <typeparamref name="TEntity"/> instance in the database via the provided <see cref="IDbConnection"/>.
    /// The entity's key will be extracted from the provided instance when building the delete command.
    /// </summary>
    /// <typeparam name="TEntity">The entity type to delete.</typeparam>
    /// <param name="connection">An open database connection.</param>
    /// <param name="entity">The entity instance to delete (its key will be extracted).</param>
    /// <returns>The number of rows affected by the delete operation (typically 1 if successful).</returns>
    public static int Delete<TEntity>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateDeleteSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            extractKey: true);
        
        return connection.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Deletes the specified <typeparamref name="TEntity"/> instance in the database via the provided <see cref="IDbTransaction"/>.
    /// The entity's key will be extracted from the provided instance when building the delete command.
    /// </summary>
    /// <typeparam name="TEntity">The entity type to delete.</typeparam>
    /// <param name="transaction">An open database transaction.</param>
    /// <param name="entity">The entity instance to delete (its key will be extracted).</param>
    /// <returns>The number of rows affected by the delete operation (typically 1 if successful).</returns>
    public static int Delete<TEntity>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateDeleteSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            extractKey: true);
        
        return transaction.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TEntity>(this IDbConnection connection, int keyValue)
        => connection.Delete<TEntity, int>(keyValue);

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TEntity>(this IDbTransaction transaction, int keyValue)
        => transaction.Delete<TEntity, int>(keyValue);

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TEntity, TKey>(this IDbConnection connection, TKey keyValue)
    {
        CommandComponents components = GenerateDeleteSql(
            [keyValue],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        
        return connection.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TEntity, TKey>(this IDbTransaction transaction, TKey keyValue)
    {
        CommandComponents components = GenerateDeleteSql(
            [keyValue],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        
        return transaction.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> in bulk using the provided array of entity
    /// instances via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="connection">An active database connection.</param>
    /// <param name="entities">An array of entities to be deleted.</param>
    /// <param name="batchSize">The number of entities to delete per batch. Defaults to 100.</param>
    /// <returns>The total number of entities successfully deleted across all batches.</returns>
    public static int BulkDelete<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulk(
            (entries, start, end)=> GenerateDeleteSql(entries, builderData, start, end, true),
            entities,
            batchSize);
    }
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> in bulk using the provided array of entity
    /// instances via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open database transaction.</param>
    /// <param name="entities">An array of entities to be deleted.</param>
    /// <param name="batchSize">The number of entities to delete per batch. Defaults to 100.</param>
    /// <returns>The total number of entities successfully deleted across all batches.</returns>
    public static int BulkDelete<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulk(
            (entries, start, end)=> GenerateDeleteSql(entries, builderData, start, end, true),
            entities,
            batchSize);
    }

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TEntity>(
        this IDbConnection connection,
        int[] keys,
        int batchSize = 100)
        => connection.BulkDelete<TEntity, int>(keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        int batchSize = 100)
        => transaction.BulkDelete<TEntity, int>(keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TEntity, TKey>(
        this IDbConnection connection,
        TKey[] keys,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulk(
            (entries, start, end) => GenerateDeleteSql(entries, builderData, start, end),
            keys,
            batchSize);
    }
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TEntity, TKey>(
        this IDbTransaction transaction,
        TKey[] keys,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateDeleteSql(entries, builderData, start, end),
            keys,
            batchSize);
    }
     
    #endregion
    
    #region Async
    
    /// <summary>
    /// Deletes the specified <typeparamref name="TEntity"/> instance in the database asynchronously via the provided
    /// <see cref="IDbConnection"/>. The entity's key will be extracted from the provided instance when
    /// building the delete command.
    /// </summary>
    /// <typeparam name="TEntity">The entity type to delete.</typeparam>
    /// <param name="connection">An open database connection.</param>
    /// <param name="entity">The entity instance to delete (its key will be extracted).</param>
    /// <returns>
    /// A task representing the asynchronous delete operation. The result contains the number of rows affected
    /// (typically 1 if successful).
    /// </returns>
    public static Task<int> DeleteAsync<TEntity>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateDeleteSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            extractKey: true);
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Deletes the specified <typeparamref name="TEntity"/> instance in the database asynchronously via the provided
    /// <see cref="IDbTransaction"/>. The entity's key will be extracted from the provided instance when
    /// building the delete command.
    /// </summary>
    /// <typeparam name="TEntity">The entity type to delete.</typeparam>
    /// <param name="transaction">An open database transaction.</param>
    /// <param name="entity">The entity instance to delete (its key will be extracted).</param>
    /// <returns>
    /// A task representing the asynchronous delete operation. The result contains the number of rows affected
    /// (typically 1 if successful).
    /// </returns>
    public static Task<int> DeleteAsync<TEntity>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateDeleteSql(
            [entity],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            extractKey: true);
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TEntity>(this IDbConnection connection, int keyValue)
        => connection.DeleteAsync<TEntity, int>(keyValue);

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TEntity>(this IDbTransaction transaction, int keyValue)
        => transaction.DeleteAsync<TEntity, int>(keyValue);

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key of type <typeparamref name="TKey"/>
    /// via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TEntity, TKey>(this IDbConnection connection, TKey keyValue)
    {
        CommandComponents components = GenerateDeleteSql(
            [keyValue],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TEntity"/> by its key of type <typeparamref name="TKey"/>
    /// via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TEntity, TKey>(this IDbTransaction transaction, TKey keyValue)
    {
        CommandComponents components = GenerateDeleteSql(
            [keyValue],
            Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> asynchronously in bulk using the provided array
    /// of entity instances via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="connection">An active database connection.</param>
    /// <param name="entities">An array of entities to be deleted.</param>
    /// <param name="batchSize">The number of entities to delete per batch. Defaults to 100.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains the total number of entities successfully deleted.
    /// </returns>
    public static Task<int> BulkDeleteAsync<TEntity>(
        this IDbConnection connection,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulkAsync(
            (entries, start, end)=> GenerateDeleteSql(entries, builderData, start, end, true),
            entities,
            batchSize);
    }
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> asynchronously in bulk using the provided array
    /// of entity instances via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open database transaction.</param>
    /// <param name="entities">An array of entities to be deleted.</param>
    /// <param name="batchSize">The number of entities to delete per batch. Defaults to 100.</param>
    /// <returns>
    /// A task representing the asynchronous operation. The result contains the total number of entities successfully deleted.
    /// </returns>
    public static Task<int> BulkDeleteAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateDeleteSql(entries, builderData, start, end, true),
            entities,
            batchSize);
    }
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TEntity>(
        this IDbConnection connection,
        int[] keys,
        int batchSize = 100)
        => connection.BulkDeleteAsync<TEntity, int>(keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        int batchSize = 100)
        => transaction.BulkDeleteAsync<TEntity, int>(keys, batchSize);

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TEntity, TKey>(
        this IDbConnection connection,
        TKey[] keys,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return connection.ExecuteBulkAsync(
            (entries, start, end)=> GenerateDeleteSql(entries, builderData, start, end),
            keys,
            batchSize);
    }

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TEntity"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to delete.</typeparam>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TEntity, TKey>(
        this IDbTransaction transaction,
        TKey[] keys,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulkAsync(
            (entries, start, end)=> GenerateDeleteSql(entries, builderData, start, end),
            keys,
            batchSize);
    }
     
    #endregion
    
    private static CommandComponents GenerateDeleteSql<T>(
        T[] keySources,
        CommandBuilderData data,
        int enumerationStartIndex = 0,
        int enumerationEnd = 1,
        bool extractKey = false)
    {
        
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        bool hasCompositeKey = data.KeyProperties.Length >1;

        if (hasCompositeKey && data.KeyProperties.Any(x => x.CompositeKeyGetter is null))
        {
            throw new CommandBuilderException(CommandBuilderExceptionType.CompositeKeyTypeNotRegistered, typeof(T).Name);
        }
        
        StringBuilder commandBuilder = new("DELETE FROM ");
        commandBuilder.AppendWithWrapper(data.DbIdentifier).Append(" WHERE ");
        
        DynamicParameters values = new();

        if (!hasCompositeKey)
        {
            PropertyMapping primaryKey = data.KeyProperties[0];
            commandBuilder.AppendWithWrapper(primaryKey.DbName, propertyWrapper);
            commandBuilder.Append(" IN (");

            for (int i = enumerationStartIndex; i < enumerationEnd; i++)
            {
                string key = string.Concat("@", primaryKey.AssemblyName, i);
                commandBuilder.AppendWithSeparator(key, ',', i==enumerationStartIndex);
                values.Add(key, extractKey ? primaryKey.Getter(keySources[i]!) : keySources[i]);
            }

            commandBuilder.Append(')');
        }
        else
        {
            for (int i = enumerationStartIndex; i < enumerationEnd; i++)
            {
                commandBuilder.AppendWithSeparator("(", " OR ", i == enumerationStartIndex);
                for (int j = 0; j < data.KeyProperties.Length; j++)
                {
                    PropertyMapping property = data.KeyProperties[j];
                    string key = string.Concat("@", property.AssemblyName, i);
                    commandBuilder.AppendEquality(property, true, propertyWrapper, i);
                    values.Add(
                        key,
                        extractKey ? property.Getter(keySources[i]!) : property.CompositeKeyGetter!(keySources[i]!));

                    if (j < data.KeyProperties.Length - 1)
                    {
                        commandBuilder.Append(" AND ");
                    }
                }
                commandBuilder.Append(')');
            }
        }
        
        return new CommandComponents(commandBuilder.ToString(), values);
    }
}