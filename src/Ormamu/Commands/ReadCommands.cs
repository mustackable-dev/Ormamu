using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;

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
        => connection.QuerySingleOrDefault<TValue>(
            GenerateSelectCommand<TValue>(keyLookup: true).Command,
            new KeyParam<int>(keyValue));
    
    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via an
    /// <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static TValue? Get<TValue>(this IDbTransaction transaction, int keyValue)
        => transaction.QuerySingleOrDefault<TValue>(
            GenerateSelectCommand<TValue>(keyLookup: true).Command,
            new KeyParam<int>(keyValue));
    
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
        SelectComponents components = GenerateSelectCommand<TValue>(keyLookup: true);
        return connection.QuerySingleOrDefault<TValue>(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
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
        SelectComponents components = GenerateSelectCommand<TValue>(keyLookup: true);
        return transaction.QuerySingleOrDefault<TValue>(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
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
            GenerateSelectCommand<TValue>(
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
            GenerateSelectCommand<TValue>(
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
        => connection.QuerySingleOrDefaultAsync<TValue>(
            GenerateSelectCommand<TValue>(keyLookup: true).Command,
            new KeyParam<int>(keyValue));
    
    /// <summary>
    /// Retrieves an entity of type <typeparamref name="TValue"/> by its key (of type <see cref="int"/>) via
    /// an <see cref="IDbTransaction"/>.
    /// </summary>
    /// <typeparam name="TValue">The type of the entity to retrieve.</typeparam>
    /// <param name="transaction">An open transaction in the database.</param>
    /// <param name="keyValue">The key value of the entity.</param>
    /// <returns>The matching entity, or <c>null</c> if not found.</returns>
    public static Task<TValue?> GetAsync<TValue>(this IDbTransaction transaction, int keyValue)
        => transaction.QuerySingleOrDefaultAsync<TValue?>(
            GenerateSelectCommand<TValue>(keyLookup: true).Command,
            new KeyParam<int>(keyValue));
    
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
        SelectComponents components = GenerateSelectCommand<TValue>(keyLookup: true);
        return connection.QuerySingleOrDefaultAsync<TValue>(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
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
        SelectComponents components = GenerateSelectCommand<TValue>(keyLookup: true);
        return transaction.QuerySingleOrDefaultAsync<TValue?>(
            components.Command,
            components.HasCompositeKey ? keyValue : new KeyParam<TKey>(keyValue));
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
            GenerateSelectCommand<TValue>(
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
            GenerateSelectCommand<TValue>(
                whereClause,
                orderByClause,
                pageSize,
                pageNumber
            ).Command,
            param);
    
    #endregion

    private sealed record SelectComponents(string Command, bool HasCompositeKey);
    private static SelectComponents GenerateSelectCommand<TValue>(
        string? whereClause = null,
        string? orderByClause = null,
        int? pageSize = null,
        int? pageNumber = null,
        bool keyLookup = false)
    {
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TValue));
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        
        StringBuilder sb = new();
        sb.Append("SELECT * FROM ").Append(data.DbIdentifier);

        if (keyLookup)
        {
            sb.Append(" WHERE ");
            if (data.KeyProperties.Length == 1)
            {
                sb.AppendEquality(data.KeyProperties[0], propertyWrapper: propertyWrapper);
            }
            else
            {
                for (int i = 0; i < data.KeyProperties.Length; i++)
                {
                    sb.AppendEquality(data.KeyProperties[i], true, propertyWrapper);
                    if (i < data.KeyProperties.Length - 1)
                    {
                        sb.Append(" AND ");
                    }
                }
            }
        }
        else
        {

            if (whereClause is not null)
            {
                sb.Append(" WHERE ").Append(whereClause);
            }

            if (orderByClause is not null)
            {
                sb.Append(" ORDER BY ").Append(orderByClause);
            }

            if (pageSize is not null && pageNumber is not null)
            {
                switch (data.Options.Dialect)
                {
                    case SqlDialect.SqlServer:
                        sb
                            .Append(" OFFSET ").Append(pageSize * (pageNumber - 1))
                            .Append(" ROWS FETCH NEXT ").Append(pageSize).Append(" ROWS ONLY");
                        break;
                    default:
                        sb.Append(" LIMIT ").Append(pageSize).Append(" OFFSET ").Append(pageSize * (pageNumber - 1));
                        break;
                }
                
            }
        }

        return new(sb.ToString(), data.KeyProperties.Length > 1);
    }
}