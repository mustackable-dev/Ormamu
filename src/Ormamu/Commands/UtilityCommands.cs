using System.Data;
using System.Linq.Expressions;
using System.Numerics;
using System.Text;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

/// <summary>
/// A collection of utility methods for common aggregations over entities in the database
/// </summary>
public static class UtilityCommands
{
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The count of matching entities</returns>
    public static TValue Count<TEntity, TValue>(
        this IDbConnection connection,
        string? whereClause = null,
        object commandParams = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(whereClause), commandParams)!;
    
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause
    /// using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The count of matching entities</returns>
    public static TValue Count<TEntity, TValue>(
        this IDbTransaction transaction,
        string? whereClause = null,
        object commandParams = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(whereClause), commandParams);
    
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The count of matching entities</returns>
    public static Task<TValue> CountAsync<TEntity, TValue>(
        this IDbConnection connection,
        string? whereClause = null,
        object commandParams = null!) where TValue: INumber<TValue>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(whereClause), commandParams)!;
    
    /// <summary>
    /// Counts the number of entities of type <typeparamref name="TEntity"/> optionally filtered by a WHERE clause
    /// using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the count result</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The count of matching entities</returns>
    public static Task<TValue> CountAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        string? whereClause = null,
        object commandParams = null!) where TValue: INumber<TValue>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(whereClause), commandParams);
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the summed property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static TValue Sum<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Sum,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result and the summed property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static TValue Sum<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.Sum<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the summed property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional SQL WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static TValue Sum<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Sum,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result and the summed property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional SQL WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static TValue Sum<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.Sum<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the summed property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static Task<TValue> SumAsync<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Sum,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result and the summed property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static Task<TValue> SumAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.SumAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the summed property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static Task<TValue> SumAsync<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Sum,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the sum of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the sum result and the summed property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to sum</param>
    /// <param name="whereClause">Optional WHERE clause to filter records</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The sum of the selected property</returns>
    public static Task<TValue> SumAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.SumAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the averaged property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static TValue Average<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Average,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result and the averaged propery</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static TValue Average<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.Average<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the averaged property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static TValue Average<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Average,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result and the averaged property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static TValue Average<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.Average<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the averaged property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static Task<TValue> AverageAsync<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Average,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result and the averaged property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static Task<TValue> AverageAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.AverageAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the averaged property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static Task<TValue> AverageAsync<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Average,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Calculates the average of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the average result and the averaged property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to average</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The average value of the selected property</returns>
    public static Task<TValue> AverageAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.AverageAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the minimized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static TValue Min<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Min,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result and the minimized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static TValue Min<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.Min<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the minimized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static TValue Min<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Min,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result and the minimized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static TValue Min<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.Min<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the minimized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static Task<TValue> MinAsync<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Min,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result and the minimized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static Task<TValue> MinAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.MinAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the minimized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static Task<TValue> MinAsync<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Min,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the minimum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the minimum result and the minimized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the minimum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The minimum value of the selected property</returns>
    public static Task<TValue> MinAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.MinAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the maximized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static TValue Max<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Max,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result and the maximized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static TValue Max<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.Max<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the maximized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static TValue Max<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalar<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Max,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result and the maximized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static TValue Max<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.Max<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the maximized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static Task<TValue> MaxAsync<TEntity, TProperty, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => connection.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Max,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/>
    /// optionally filtered by a WHERE clause
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result and the maximized property</typeparam>
    /// <param name="connection">The database connection</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static Task<TValue> MaxAsync<TEntity, TValue>(
        this IDbConnection connection,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => connection.MaxAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result</typeparam>
    /// <typeparam name="TProperty">The numeric type of the maximized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static Task<TValue> MaxAsync<TEntity, TProperty, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TProperty>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
    where TProperty : INumber<TProperty>
        => transaction.ExecuteScalarAsync<TValue>(GenerateUtilitySql<TEntity>(
            whereClause,
            UtilityType.Max,
            propertySelector), commandParams)!;
    
    /// <summary>
    /// Gets the maximum value of the specified property for entities of type <typeparamref name="TEntity"/> optionally
    /// filtered by a WHERE clause using a transaction
    /// </summary>
    /// <typeparam name="TEntity">The entity type</typeparam>
    /// <typeparam name="TValue">The numeric type of the maximum result and the maximized property</typeparam>
    /// <param name="transaction">The database transaction</param>
    /// <param name="propertySelector">An expression selecting the property to find the maximum value of</param>
    /// <param name="whereClause">Optional WHERE clause to filter records (without the WHERE keyword)</param>
    /// <param name="commandParams">An optional object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here</param>
    /// <returns>The maximum value of the selected property</returns>
    public static Task<TValue> MaxAsync<TEntity, TValue>(
        this IDbTransaction transaction,
        Expression<Func<TEntity, TValue>> propertySelector,
        string? whereClause = null,
        object commandParams = null!)
    where TValue: INumber<TValue>
        => transaction.MaxAsync<TEntity, TValue, TValue>(propertySelector, whereClause, commandParams);

    private static string GenerateUtilitySql<TEntity>(
        string? whereClause = null,
        UtilityType type = UtilityType.Count,
        Expression? propertySelector = null)
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

    private static string ResolvePropertyName(CommandBuilderData data, Expression propertyExpression)
    {
        char propertyWrapper = data.Options.Dialect switch
        {
            SqlDialect.PostgreSql => '"',
            SqlDialect.MySql or SqlDialect.MariaDb => '`',
            _=> '\0'
        };
        string assemblyName = propertyExpression.ParsePropertyName();
        string dbName = data.Properties.First(x => x.AssemblyName == assemblyName).DbName;
        if (propertyWrapper != '\0') return string.Concat(propertyWrapper, dbName, propertyWrapper);
        return dbName;
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