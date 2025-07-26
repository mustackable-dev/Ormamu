using System.Data;
using System.Linq.Expressions;
using System.Numerics;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

public static class UtilityCommands
{
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The count of matching entities.</returns>
    public static TValue Count<TEntity, TValue>(
        this IDbConnection connection,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(whereClause), param)!;
    
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause
    /// using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The count of matching entities.</returns>
    public static TValue Count<TEntity, TValue>(
        this IDbTransaction transaction,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(whereClause), param);
    
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The count of matching entities.</returns>
    public static Task<TValue> CountAsync<TEntity, TValue>(
        this IDbConnection connection,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(whereClause), param)!;
    
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause
    /// using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The count of matching entities.</returns>
    public static Task<TValue> CountAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(whereClause), param);
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to sum.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The sum of the selected property.</returns>
    public static TValue Sum<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Sum,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to sum.</param>
    /// <param name="whereClause">Optional SQL WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The sum of the selected property.</returns>
    public static TValue Sum<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Sum,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to sum.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The sum of the selected property.</returns>
    public static Task<TValue> SumAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Sum,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to sum.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The sum of the selected property.</returns>
    public static Task<TValue> SumAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Sum,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to average.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The average value of the selected property.</returns>
    public static TValue Average<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Average,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to average.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The average value of the selected property.</returns>
    public static TValue Average<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Average,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to average.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The average value of the selected property.</returns>
    public static Task<TValue> AverageAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Average,
            propertySelector), param)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to average.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The average value of the selected property.</returns>
    public static Task<TValue> AverageAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Average,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The minimum value of the selected property.</returns>
    public static TValue Min<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Min,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The minimum value of the selected property.</returns>
    public static TValue Min<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Min,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The minimum value of the selected property.</returns>
    public static Task<TValue> MinAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Min,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The minimum value of the selected property.</returns>
    public static Task<TValue> MinAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Min,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The maximum value of the selected property.</returns>
    public static TValue Max<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Max,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The maximum value of the selected property.</returns>
    public static TValue Max<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Max,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result.</typeparam>
    /// <param name="connection">The database connection.</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The maximum value of the selected property.</returns>
    public static Task<TValue> MaxAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Max,
            propertySelector), param)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction.
    /// </summary>
    /// <typeparam name="TEntity">The entity type.</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result.</typeparam>
    /// <param name="transaction">The database transaction.</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of.</param>
    /// <param name="whereClause">Optional WHERE clause to filter records.</param>
    /// <param name="param">Parameters to use for the query.</param>
    /// <returns>The maximum value of the selected property.</returns>
    public static Task<TValue> MaxAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, object>> propertySelector,
        string? whereClause = null,
        object param = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql(
            whereClause,
            UtilityType.Max,
            propertySelector), param)!;

    private static string GenerateUtilitySql<TEntity>(
        string? whereClause = null,
        UtilityType type = UtilityType.Count,
        Expression<Func<TEntity, object>>? propertySelector = null)
    {
        CommandBuilderData data = Cache.ResolveCommandBuilderData(typeof(TEntity));
        string columnIdentifier = propertySelector is null ? "*" : ResolvePropertyName(data, propertySelector);
        StringBuilder sb = new ("SELECT ");
        
        sb.Append(type switch
        {
            UtilityType.Count => "COUNT(",
            UtilityType.Sum => "SUM(",
            UtilityType.Average => "AVG(",
            UtilityType.Min => "MIN(",
            UtilityType.Max => "MAX(",
            _=>string.Empty,
        });

        sb.Append(columnIdentifier).Append(")").Append(" FROM ").Append(data.DbIdentifier);
        
        if (whereClause != null)
        {
            sb.Append(" WHERE ");
            sb.Append(whereClause);
        }
        return sb.ToString();
    }

    private static string ResolvePropertyName<T>(CommandBuilderData data, Expression<Func<T, object>> expression)
    {
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        string assemblyName = expression.ParsePropertyName();
        string dbName = data.Properties.First(x => x.AssemblyName == assemblyName).DbName;
        if (propertyWrapper != '\0') return string.Concat(propertyWrapper, dbName, propertyWrapper);
        return dbName;
    }
    private static string ParsePropertyName<T>(this Expression<Func<T, object>> expression)
    {
        string rawExpression = expression.Body is UnaryExpression { Operand: MemberExpression propertyExpression } ? 
            propertyExpression.ToString():
            expression.ToString();
        
        return rawExpression[(rawExpression.IndexOf('.') + 1)..];
    }

    private enum UtilityType
    {
        Count,
        Sum,
        Average,
        Min,
        Max
    }
}