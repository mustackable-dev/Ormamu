using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;
using Ormamu.Exceptions;

namespace Ormamu;

public static class ReadCommands
{
    #region Regular

    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static TValue? Get<TValue>(this IDbConnection connection, int keyValue)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>([keyValue]);
        return connection.QuerySingleOrDefault<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static TValue? Get<TValue>(this IDbTransaction transaction, int keyValue)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>([keyValue]);
        return transaction.QuerySingleOrDefault<TValue>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the entity's key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static TValue? Get<TKey, TValue>(this IDbConnection connection, TKey keyValue)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>([keyValue]);
        return connection.QuerySingleOrDefault<TValue>(components.Command,components.Parameters);
    }

    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the entity's key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static TValue? Get<TKey, TValue>(this IDbTransaction transaction, TKey keyValue)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>([keyValue]);
        return transaction.QuerySingleOrDefault<TValue>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of integer primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static IEnumerable<TValue> Get<TValue>(
        this IDbConnection connection,
        int[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return connection.Query<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of integer primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static IEnumerable<TValue> Get<TValue>(
        this IDbTransaction transaction,
        int[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return transaction.Query<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the primary key values.</typeparam>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static IEnumerable<TValue> Get<TKey, TValue>(
        this IDbConnection connection,
        TKey[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return connection.Query<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the primary key values.</typeparam>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static IEnumerable<TValue> Get<TKey, TValue>(
        this IDbTransaction transaction,
        TKey[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return transaction.Query<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database using optional filtering,
    /// ordering, and pagination via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="whereClause">An optional WHERE clause to filter the results.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>A collection of matching entities.</returns>
    public static IEnumerable<TValue> Get<TValue>(
        this IDbConnection connection,
        string? whereClause = null,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null,
        object param = null!)
    
        => connection.Query<TValue>(
            GenerateSelectCommand<int, TValue>(
                [],
                whereClause,
                orderByClause,
                pageSize,
                pageNumber
            ).Command, param);

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database using optional filtering,
    /// ordering, and pagination via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="whereClause">An optional WHERE clause to filter the results.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>A collection of matching entities.</returns>
    public static IEnumerable<TValue> Get<TValue>(
        this IDbTransaction transaction,
        string? whereClause = null,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null,
        object param = null!)
    
        => transaction.Query<TValue>(
            GenerateSelectCommand<int, TValue>(
                [],
                whereClause,
                orderByClause,
                pageSize,
                pageNumber
            ).Command,param);

    #endregion
    
    #region Async

    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static Task<TValue?> GetAsync<TValue>(this IDbConnection connection, int keyValue)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>([keyValue]);
        return connection.QuerySingleOrDefaultAsync<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via
    /// an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static Task<TValue?> GetAsync<TValue>(this IDbTransaction transaction, int keyValue)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>([keyValue]);
        return transaction.QuerySingleOrDefaultAsync<TValue?>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/> via
    /// an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the entity's key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static Task<TValue?> GetAsync<TKey, TValue>(this IDbConnection connection, TKey keyValue)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>([keyValue]);
        return connection.QuerySingleOrDefaultAsync<TValue>(components.Command,components.Parameters);
    }

    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key of type <typeparamref name="TKey"/> via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the entity's key.</typeparam>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static Task<TValue?> GetAsync<TKey, TValue>(this IDbTransaction transaction, TKey keyValue)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>([keyValue]);
        return transaction.QuerySingleOrDefaultAsync<TValue?>(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of integer primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static Task<IEnumerable<TValue>> GetAsync<TValue>(
        this IDbConnection connection,
        int[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return connection.QueryAsync<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of integer primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static Task<IEnumerable<TValue>> GetAsync<TValue>(
        this IDbTransaction transaction,
        int[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<int, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return transaction.QueryAsync<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the primary key values.</typeparam>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="keys">An array of primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static Task<IEnumerable<TValue>> GetAsync<TKey, TValue>(
        this IDbConnection connection,
        TKey[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return connection.QueryAsync<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database by primary key values,
    /// with optional ordering and pagination, via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TKey">The type of the primary key values.</typeparam>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keys">An array of primary key values to match.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <returns>A collection of matching entities.</returns>
    public static Task<IEnumerable<TValue>> GetAsync<TKey, TValue>(
        this IDbTransaction transaction,
        TKey[] keys,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        SelectComponents components = GenerateSelectCommand<TKey, TValue>(
            keys,
            null,
            orderByClause,
            pageSize,
            pageNumber
        );
        return transaction.QueryAsync<TValue>(components.Command, components.Parameters);
    }

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database using optional filtering,
    /// ordering, and pagination via an <see cref="IDbConnection"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="connection">A connection to the database.</param>
    /// <param name="whereClause">An optional WHERE clause to filter the results.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>A collection of matching entities.</returns>
    public static Task<IEnumerable<TValue>> GetAsync<TValue>(
        this IDbConnection connection,
        string? whereClause = null,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null,
        object param = null!)
    
        => connection.QueryAsync<TValue>(
            GenerateSelectCommand<int, TValue>(
                [],
                whereClause,
                orderByClause,
                pageSize,
                pageNumber
            ).Command,
            param);

    /// <summary>
    /// Retrieves a list of entities of type <typeparamref name="TValue"/> from the database using optional filtering,
    /// ordering, and pagination via an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entities to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="whereClause">An optional WHERE clause to filter the results.</param>
    /// <param name="orderByClause">An optional ORDER BY clause.</param>
    /// <param name="pageSize">The number of results per page (for pagination).</param>
    /// <param name="pageNumber">The page number to retrieve (for pagination).</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>A collection of matching entities.</returns>
    public static Task<IEnumerable<TValue>> GetAsync<TValue>(
        this IDbTransaction transaction,
        string? whereClause = null,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null,
        object param = null!)
        
        => transaction.QueryAsync<TValue>(
            GenerateSelectCommand<int, TValue>(
                [],
                whereClause,
                orderByClause,
                pageSize,
                pageNumber
            ).Command,
            param);
    
    #endregion

    private sealed record SelectComponents(string Command, DynamicParameters Parameters);
    private static SelectComponents GenerateSelectCommand<TKey, TValue>(
        TKey[] keys,
        string? whereClause = null,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null)
    {
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        StringBuilder commandBuilder = new();
        commandBuilder.Append("SELECT * FROM ").Append(data.DbIdentifier);
        DynamicParameters values = new();

        if (keys.Length>0)
        {
        
            bool hasCompositeKey = data.KeyProperties.Length>1;

            if (hasCompositeKey && data.KeyProperties.Any(x => x.CompositeKeyGetter is null))
            {
                throw new CommandBuilderException(CommandBuilderExceptionType.CompositeKeyTypeNotRegistered, typeof(TKey).Name);
            }
            
            commandBuilder.Append(" WHERE ");
            if (!hasCompositeKey)
            {
                commandBuilder.AppendWithWrapper(data.KeyProperties[0].DbName, propertyWrapper);
                commandBuilder.Append(" IN (");

                for (int i = 0; i < keys.Length; i++)
                {
                    string key = string.Concat("@", data.KeyProperties[0].AssemblyName, i);
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
                    for (int j = 0; j < data.KeyProperties.Length; j++)
                    {
                        PropertyMapping property = data.KeyProperties[j];
                        string key = string.Concat("@", property.AssemblyName, i);
                        commandBuilder.AppendEquality(property, true, propertyWrapper, i);
                        values.Add(key, property.CompositeKeyGetter(keys[i]!));

                        if (j < data.KeyProperties.Length - 1)
                        {
                            commandBuilder.Append(" AND ");
                        }
                    }
                    commandBuilder.Append(')');
                }
            }
        }
        else
        {

            if (whereClause is not null)
            {
                commandBuilder.Append(" WHERE ").Append(whereClause);
            }

            if (orderByClause is not null)
            {
                commandBuilder.Append(" ORDER BY ").Append(orderByClause);
            }

            if (pageSize is not null && pageNumber is not null)
            {
                switch (data.Options.Dialect)
                {
                    case SqlDialect.SqlServer:
                        commandBuilder
                            .Append(" OFFSET ").Append(pageSize * (pageNumber - 1))
                            .Append(" ROWS FETCH NEXT ").Append(pageSize).Append(" ROWS ONLY");
                        break;
                    default:
                        commandBuilder.Append(" LIMIT ").Append(pageSize).Append(" OFFSET ").Append(pageSize * (pageNumber - 1));
                        break;
                }
                
            }
        }

        return new(commandBuilder.ToString(), values);
    }
}