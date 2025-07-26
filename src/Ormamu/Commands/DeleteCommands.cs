using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

public static class DeleteCommands
{
    #region Regular
    
    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TValue>(this IDbConnection connection, int keyValue)
        => connection.Execute(GenerateDeleteSql<TValue>().Command, new KeyParam<int>(keyValue));

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TValue>(this IDbTransaction transaction, int keyValue)
        => transaction.Execute(GenerateDeleteSql<TValue>().Command, new KeyParam<int>(keyValue));

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TKey, TValue>(this IDbConnection connection, TKey keyValue)
    {
        DeleteComponents components = GenerateDeleteSql<TValue>();
        return connection.Execute(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
    }

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static int Delete<TKey, TValue>(this IDbTransaction transaction, TKey keyValue)
    {
        DeleteComponents components = GenerateDeleteSql<TValue>();
        return transaction.Execute(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
    }

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TValue>(
        this IDbConnection connection,
        int[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithConnection<int, TValue>(connection, keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TValue>(
        this IDbTransaction transaction,
        int[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithTransaction<int, TValue>(transaction, keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TKey, TValue>(
        this IDbConnection connection,
        TKey[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithConnection<TKey, TValue>(connection, keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static int BulkDelete<TKey, TValue>(
        this IDbTransaction transaction,
        TKey[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithTransaction<TKey, TValue>(transaction, keys, batchSize);

    private static int RunBulkDeleteWithConnection<TKey, TValue>(
        IDbConnection connection,
        TKey[] keys,
        int batchSize = 100)
    {
        int deletedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = keys.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkDeleteComponents components =
                GenerateBulkDeleteSql(
                    keys.AsSpan(deletedEntries, currentBatchSize),
                    data);
            deletedEntries += connection.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return deletedEntries;
    }

    private static int RunBulkDeleteWithTransaction<TKey, TValue>(
        this IDbTransaction transaction,
        TKey[] keys,
        int batchSize = 100)
    {
        int deletedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = keys.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkDeleteComponents components =
                GenerateBulkDeleteSql(
                    keys.AsSpan(deletedEntries, currentBatchSize),
                    data);
            deletedEntries += transaction.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return deletedEntries;
    }
     
    #endregion
    
    #region Async

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TValue>(this IDbConnection connection, int keyValue)
        => connection.ExecuteAsync(
            GenerateDeleteSql<TValue>().Command,
            new KeyParam<int>(keyValue));

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TValue>(this IDbTransaction transaction, int keyValue)
        => transaction.ExecuteAsync(
            GenerateDeleteSql<TValue>().Command,
            new KeyParam<int>(keyValue));

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/>
    /// via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TKey, TValue>(this IDbConnection connection, TKey keyValue)
    {
        DeleteComponents components = GenerateDeleteSql<TValue>();
        return connection.ExecuteAsync(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
    }

    /// <summary>
    /// Deletes an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/>
    /// via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The number of affected rows (typically 1 if deletion was successful).</returns>
    public static Task<int> DeleteAsync<TKey, TValue>(this IDbTransaction transaction, TKey keyValue)
    {
        DeleteComponents components = GenerateDeleteSql<TValue>();
        return transaction.ExecuteAsync(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
    }

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TValue>(
        this IDbConnection connection,
        int[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithConnectionAsync<int, TValue>(connection, keys, batchSize);
    
    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys
    /// (of type <see cref="int"/>) via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TValue>(
        this IDbTransaction transaction,
        int[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithTransactionAsync<int, TValue>(transaction, keys, batchSize);

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TKey, TValue>(
        this IDbConnection connection,
        TKey[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithConnectionAsync<TKey, TValue>(connection, keys, batchSize);

    /// <summary>
    /// Deletes multiple entities of type <typeparamref name="TValue"/> using an array of keys of type
    /// <typeparamref name="TKey"/> via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the key.</typeparam>
    /// <typeparam name="TValue">The type of the entities to delete.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of key values representing the entities to delete.</param>
    /// <param name="batchSize">The number of deletions to perform per batch.</param>
    /// <returns>The number of entities successfully deleted.</returns>
    public static Task<int> BulkDeleteAsync<TKey, TValue>(
        this IDbTransaction transaction,
        TKey[] keys,
        int batchSize = 100)
        => RunBulkDeleteWithTransactionAsync<TKey, TValue>(transaction, keys, batchSize);

    private static async Task<int> RunBulkDeleteWithConnectionAsync<TKey, TValue>(
        IDbConnection connection,
        TKey[] keys,
        int batchSize = 100)
    {
        int deletedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = keys.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkDeleteComponents components =
                GenerateBulkDeleteSql(
                    keys.AsSpan(deletedEntries, currentBatchSize),
                    data);
            deletedEntries += await connection.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return deletedEntries;
    }

    private static async Task<int> RunBulkDeleteWithTransactionAsync<TKey, TValue>(
        this IDbTransaction transaction,
        TKey[] keys,
        int batchSize = 100)
    {
        int deletedEntries = 0;
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        
        int remaining = keys.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            BulkDeleteComponents components =
                GenerateBulkDeleteSql(
                    keys.AsSpan(deletedEntries, currentBatchSize),
                    data);
            deletedEntries += await transaction.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return deletedEntries;
    }
     
    #endregion
    
    private sealed record DeleteComponents(string Command, bool HasCompositeKey);
    private static DeleteComponents GenerateDeleteSql<TValue>()
    {
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        StringBuilder commandBuilder = new StringBuilder("DELETE FROM ").Append(data.DbIdentifier).Append(" WHERE ");
        
        if (data.CompositeKeyData is null)
        {
            commandBuilder.AppendEquality(data.PrimaryKey, propertyWrapper: propertyWrapper);
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

        return new(commandBuilder.ToString(), data.CompositeKeyData is not null);
    }
    
    private sealed record BulkDeleteComponents(string Command, DynamicParameters Parameters);
    
    private static BulkDeleteComponents GenerateBulkDeleteSql<TKey>(
        Span<TKey> keys,
        CommandBuilderData data)
    {
        
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        bool hasCompositeKey = data.CompositeKeyData is not null;
        
        StringBuilder commandBuilder = new("DELETE FROM ");
        commandBuilder.AppendWithWrapper(data.DbIdentifier).Append(" WHERE ");
        
        DynamicParameters values = new();

        if (!hasCompositeKey)
        {
            commandBuilder.AppendWithWrapper(data.PrimaryKey.DbName, propertyWrapper);
            commandBuilder.Append(" IN (");

            for (int i = 0; i < keys.Length; i++)
            {
                string key = string.Concat("@", data.PrimaryKey.AssemblyName, i);
                commandBuilder.AppendWithSeparator(key, ',', i==0);
                values.Add(key, keys[i]);
                
            }

            commandBuilder.Append(')');
        }
        else
        {
            for (int i = 0; i < keys.Length; i++)
            {
                commandBuilder.AppendWithSeparator("(", " OR ", i == 0);
                for (int j = 0; j < data.CompositeKeyData!.Properties.Length; j++)
                {
                    PropertyMapping property = data.CompositeKeyData.Properties[j];
                    string key = string.Concat("@", property.AssemblyName, i);
                    commandBuilder.AppendEquality(property, true, propertyWrapper, i);
                    values.Add(key, property.Getter(keys[i]!));

                    if (j < data.CompositeKeyData.Properties.Length - 1)
                    {
                        commandBuilder.Append(" AND ");
                    }
                }
                commandBuilder.Append(')');
            }
        }
        
        return new BulkDeleteComponents(commandBuilder.ToString(), values);
    }
}