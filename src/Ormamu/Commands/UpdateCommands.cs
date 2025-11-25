using System.Data;
using System.Text;
using Dapper;
using Dapper.Transaction;
using Ormamu.Exceptions;

namespace Ormamu;

/// <summary>
/// A collection of utility methods for updating entities in the database
/// </summary>
public static class UpdateCommands
{
    
    #region Regular

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity with updated values</param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static int Update<TEntity>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return connection.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity with updated values</param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static int Update<TEntity>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return transaction.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">The array of entities with updated values</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
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
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an
    /// <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">The array of entities with updated values</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkUpdate<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulk(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }
    
    /// <summary>
    /// Updates one or more entities of type <typeparamref name="TEntity"/> using a custom WHERE clause with
    /// an <see cref="IDbConnection"/>. The values to update are taken from the <paramref name="payload"/> object.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="payload">An object containing the values to update</param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The number of affected rows</returns>
    public static int BulkUpdate<TEntity>(
        this IDbConnection connection,
        TEntity payload,
        string whereClause,
        object commandParams = null!)
    {
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            whereClause: whereClause,
            initialParams: initialParams);
        
        return connection.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Updates one or more entities of type <typeparamref name="TEntity"/> using a custom WHERE clause within
    /// an <see cref="IDbTransaction"/>. The values to update are taken from the <paramref name="payload"/> object.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="payload">An object containing the values to update</param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The number of affected rows</returns>
    public static int BulkUpdate<TEntity>(
        this IDbTransaction transaction,
        TEntity payload,
        string whereClause,
        object commandParams = null!)
    {
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            whereClause: whereClause,
            initialParams: initialParams);
        
        return transaction.Execute(components.Command, components.Parameters);
    }
     
    #endregion
    
    #region Async

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity with updated values</param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static Task<int> UpdateAsync<TEntity>(this IDbConnection connection, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity with updated values</param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static Task<int> UpdateAsync<TEntity>(this IDbTransaction transaction, TEntity entity)
    {
        CommandComponents components = GenerateUpdateSql([entity], Cache.ResolveCommandBuilderData(typeof(TEntity)));
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbConnection"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">The array of entities with updated values</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
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
    /// Updates multiple entities of type <typeparamref name="TEntity"/> in batches using an <see cref="IDbTransaction"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">The array of entities with updated values</param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity[] entities,
        int batchSize = 100)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        return transaction.ExecuteBulkAsync(
            (entries, start, end) => GenerateUpdateSql(entries, builderData, start, end), entities, batchSize);
    }
    
    /// <summary>
    /// Asynchronously updates one or more entities of type <typeparamref name="TEntity"/> using a custom WHERE clause with
    /// an <see cref="IDbConnection"/>. The values to update are taken from the <paramref name="payload"/> object.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="payload">An object containing the values to update</param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The number of affected rows</returns>
    public static Task<int> BulkUpdateAsync<TEntity>(
        this IDbConnection connection,
        TEntity payload,
        string whereClause,
        object commandParams = null!)
    {
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            whereClause: whereClause,
            initialParams: initialParams);
        
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Asynchronously updates one or more entities of type <typeparamref name="TEntity"/> using a custom WHERE clause within
    /// an <see cref="IDbTransaction"/>. The values to update are taken from the <paramref name="payload"/> object.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="payload">An object containing the values to update</param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The number of affected rows</returns>
    public static Task<int> BulkUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity payload,
        string whereClause,
        object commandParams = null!)
    {
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            whereClause: whereClause,
            initialParams: initialParams);
        
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
     
    #endregion
    
    #region PartialRegular
    
    /// <summary>
    /// Partially updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbConnection"/>.
    /// Only the properties specified by the <paramref name="propertySettersFactory"/> are updated, and their values
    /// are copied from the provided <paramref name="entity"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>The number of affected rows (typically 1)</returns>
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
    
    /// <summary>
    /// Partially updates an existing entity of type <typeparamref name="TEntity"/> using an <see cref="IDbTransaction"/>.
    /// Only the properties specified by the <paramref name="propertySettersFactory"/> are updated, and their values
    /// are copied from the provided <paramref name="entity"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>The number of affected rows (typically 1)</returns>
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
    
    /// <summary>
    /// Partially updates an existing entity of type <typeparamref name="TEntity"/> identified by its integer key
    /// using an <see cref="IDbConnection"/>. Only the properties specified by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="key">The primary key value of the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static int PartialUpdate<TEntity>(
        this IDbConnection connection,
        int key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => connection.PartialUpdate<TEntity, int>(key, propertySettersFactory);

    /// <summary>
    /// Partially updates an existing entity of type <typeparamref name="TEntity"/> identified by its integer key
    /// using an <see cref="IDbTransaction"/>. Only the properties specified by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="key">The primary key value of the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static int PartialUpdate<TEntity>(
        this IDbTransaction transaction,
        int key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => transaction.PartialUpdate<TEntity, int>(key, propertySettersFactory);
    
    /// <summary>
    /// Partially updates an existing entity of type <typeparamref name="TEntity"/> identified by a key of type
    /// <typeparamref name="TKey"/> using an <see cref="IDbConnection"/>. Only the properties specified by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="key">The key value identifying the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static int PartialUpdate<TEntity, TKey>(
        this IDbConnection connection,
        TKey key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [key],
            builderData,
            partials: partials);
        return connection.Execute(components.Command, components.Parameters);
    }

    /// <summary>
    /// Partially updates an existing entity of type <typeparamref name="TEntity"/> identified by a key of type
    /// <typeparamref name="TKey"/> using an <see cref="IDbTransaction"/>. Only the properties specified by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="key">The key value identifying the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>The number of affected rows (typically 1)</returns>
    public static int PartialUpdate<TEntity, TKey>(
        this IDbTransaction transaction,
        TKey key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [key],
            builderData,
            partials: partials);
        return transaction.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Partially updates multiple entities of type <typeparamref name="TEntity"/> in batches using an
    /// <see cref="IDbConnection"/>. The property values used in the update are copied from each entity in
    /// <paramref name="entities"/>. Only the properties selected by the <paramref name="propertySettersFactory"/>
    /// are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">The array of entities whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
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
    
    /// <summary>
    /// Partially updates multiple entities of type <typeparamref name="TEntity"/> in batches using an
    /// <see cref="IDbTransaction"/>. The property values used in the update are copied from each entity in
    /// <paramref name="entities"/>. Only the properties selected by the <paramref name="propertySettersFactory"/>
    /// are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">The array of entities whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
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
    
    /// <summary>
    /// Partially updates multiple entities of type <typeparamref name="TEntity"/> identified by their integer keys
    /// in batches using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity>(
        this IDbConnection connection,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => connection.BulkPartialUpdate<TEntity, int>(keys, propertySettersFactory, batchSize);

    /// <summary>
    /// Partially updates multiple entities of type <typeparamref name="TEntity"/> identified by their integer keys
    /// in batches using an <see cref="IDbTransaction"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => transaction.BulkPartialUpdate<TEntity, int>(keys, propertySettersFactory, batchSize);
    
    /// <summary>
    /// Partially updates multiple entities of type <typeparamref name="TEntity"/> identified by keys of type
    /// <typeparamref name="TKey"/> in batches using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity, TKey>(
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

    /// <summary>
    /// Partially updates multiple entities of type <typeparamref name="TEntity"/> identified by keys of type
    /// <typeparamref name="TKey"/> in batches using an <see cref="IDbTransaction"/>. Only the properties selected
    /// by the <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity, TKey>(
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
    
    /// <summary>
    /// Partially copies values into one or multiple entities of type <typeparamref name="TEntity"/> using an
    /// <see cref="IDbConnection"/>. Only the properties selected by the <paramref name="propertySettersFactory"/> are
    /// copied, and the copy operation is constrained by the specified <paramref name="whereClause"/>. The values are
    /// copied from <paramref name="payload"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to copy into</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="payload">An entity instance providing the values to copy</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to copy by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity>(
        this IDbConnection connection,
        TEntity payload,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return connection.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Partially copies values into one or multiple entities of type <typeparamref name="TEntity"/> using an
    /// <see cref="IDbTransaction"/>. Only the properties selected by the <paramref name="propertySettersFactory"/>
    /// are copied, and the copy operation is constrained by the specified <paramref name="whereClause"/>. The values
    /// are copied from <paramref name="payload"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to copy into</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="payload">An entity instance providing the values to copy</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to copy by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity>(
        this IDbTransaction transaction,
        TEntity payload,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return transaction.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified by integer primary keys
    /// using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of integer primary keys identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity>(
        this IDbConnection connection,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
        => BulkPartialUpdate<TEntity, int>(connection, keys, propertySettersFactory, whereClause, commandParams);
    
    /// <summary>
    /// Partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified by integer primary keys
    /// using an <see cref="IDbTransaction"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of integer primary keys identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
        => BulkPartialUpdate<TEntity, int>(transaction, keys, propertySettersFactory, whereClause, commandParams);
    
    /// <summary>
    /// Partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified by keys of type
    /// <typeparamref name="TKey"/> using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static int BulkPartialUpdate<TEntity, TKey>(
        this IDbConnection connection,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            keys,
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return connection.Execute(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified by keys of type
    /// <typeparamref name="TKey"/> using an <see cref="IDbTransaction"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>

    public static int BulkPartialUpdate<TEntity, TKey>(
        this IDbTransaction transaction,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            keys,
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return transaction.Execute(components.Command, components.Parameters);
    }
    
    #endregion
    
    #region PartialAsync
    
    /// <summary>
    /// Asynchronously and partially updates an existing entity of type <typeparamref name="TEntity"/> using an
    /// <see cref="IDbConnection"/>. Only the properties specified by the <paramref name="propertySettersFactory"/>
    /// are updated, and their values are copied from the provided <paramref name="entity"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entity">The entity whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>A task representing the asynchronous operation, containing the number of affected rows</returns>
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
    
    /// <summary>
    /// Asynchronously and partially updates an existing entity of type <typeparamref name="TEntity"/> using an
    /// <see cref="IDbTransaction"/>. Only the properties specified by the <paramref name="propertySettersFactory"/>
    /// are updated, and their values are copied from the provided <paramref name="entity"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entity">The entity whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>A task representing the asynchronous operation, containing the number of affected rows</returns>
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
    
    /// <summary>
    /// Asynchronously and partially updates an existing entity of type <typeparamref name="TEntity"/> identified by
    /// its integer key using an <see cref="IDbConnection"/>. Only the properties specified by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="key">The primary key identifying the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>A task representing the asynchronous operation, containing the number of affected rows</returns>
    public static Task<int> PartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        int key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => connection.PartialUpdateAsync<TEntity, int>(key, propertySettersFactory);

    /// <summary>
    /// Asynchronously and partially updates an existing entity of type <typeparamref name="TEntity"/> identified by
    /// its integer key using an <see cref="IDbTransaction"/>. Only the properties specified by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="key">The primary key identifying the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>A task representing the asynchronous operation, containing the number of affected rows</returns>
    public static Task<int> PartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        int key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
        => transaction.PartialUpdateAsync<TEntity, int>(key, propertySettersFactory);
    
    /// <summary>
    /// Asynchronously and partially updates an existing entity of type <typeparamref name="TEntity"/> identified by
    /// a key of type <typeparamref name="TKey"/> using an <see cref="IDbConnection"/>. Only the properties specified
    /// by the <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="key">The key identifying the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>A task representing the asynchronous operation, containing the number of affected rows</returns>
    public static Task<int> PartialUpdateAsync<TEntity, TKey>(
        this IDbConnection connection,
        TKey key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [key],
            builderData,
            partials: partials);
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }

    /// <summary>
    /// Asynchronously and partially updates an existing entity of type <typeparamref name="TEntity"/> identified by
    /// a key of type <typeparamref name="TKey"/> using an <see cref="IDbTransaction"/>. Only the properties specified
    /// by the <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entity to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="key">The key identifying the entity to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <returns>A task representing the asynchronous operation, containing the number of affected rows</returns>
    public static Task<int> PartialUpdateAsync<TEntity, TKey>(
        this IDbTransaction transaction,
        TKey key,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        CommandComponents components = GenerateUpdateSql<TKey>(
            [key],
            builderData,
            partials: partials);
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Asynchronously and partially updates multiple entities of type <typeparamref name="TEntity"/> in batches using
    /// an <see cref="IDbConnection"/>. The property values used in each update are copied from each entity in
    /// <paramref name="entities"/>. Only the properties selected by the <paramref name="propertySettersFactory"/>
    /// are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="entities">The array of entities whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>A task representing the asynchronous operation, containing the total number of affected rows</returns>
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
    
    /// <summary>
    /// Asynchronously and partially updates multiple entities of type <typeparamref name="TEntity"/> in batches using
    /// an <see cref="IDbTransaction"/>. The property values used in each update are copied from each entity in
    /// <paramref name="entities"/>. Only the properties selected by the <paramref name="propertySettersFactory"/>
    /// are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="entities">The array of entities whose property values will be copied into the update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>A task representing the asynchronous operation, containing the total number of affected rows</returns>
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
    
    /// <summary>
    /// Asynchronously and partially updates multiple entities of type <typeparamref name="TEntity"/> identified by
    /// their integer keys in batches using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>A task representing the asynchronous operation, containing the total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => connection.BulkPartialUpdateAsync<TEntity, int>(keys, propertySettersFactory, batchSize);

    /// <summary>
    /// Asynchronously and partially updates multiple entities of type <typeparamref name="TEntity"/> identified by
    /// their integer keys in batches using an <see cref="IDbTransaction"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparamref name="TEntity">The type of the entities to update</typeparamref>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>A task representing the asynchronous operation, containing the total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        int batchSize = 100)
        => transaction.BulkPartialUpdateAsync<TEntity, int>(keys, propertySettersFactory, batchSize);
    
    /// <summary>
    /// Asynchronously and partially updates multiple entities of type <typeparamref name="TEntity"/> identified by
    /// keys of type <typeparamref name="TKey"/> in batches using an <see cref="IDbConnection"/>. Only the properties
    /// selected by the <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>A task representing the asynchronous operation, containing the total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity, TKey>(
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

    /// <summary>
    /// Asynchronously and partially updates multiple entities of type <typeparamref name="TEntity"/> identified by
    /// keys of type <typeparamref name="TKey"/> in batches using an <see cref="IDbTransaction"/>. Only the properties
    /// selected by the <paramref name="propertySettersFactory"/> are updated
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="batchSize">The number of entities to update per batch. Defaults to 100</param>
    /// <returns>A task representing the asynchronous operation, containing the total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity, TKey>(
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
    
    /// <summary>
    /// Asynchronously and partially copies values into one or multiple entities of type <typeparamref name="TEntity"/>
    /// using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are copied, and the copy operation is constrained by the specified
    /// <paramref name="whereClause"/>. The values are copied from <paramref name="payload"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to copy into</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="payload">An entity instance providing the values to copy</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to copy by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        TEntity payload,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Asynchronously and partially copies values into one or multiple entities of type <typeparamref name="TEntity"/>
    /// using an <see cref="IDbTransaction"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are copied, and the copy operation is constrained by the specified
    /// <paramref name="whereClause"/>. The values are copied from <paramref name="payload"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to copy into</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="payload">An entity instance providing the values to copy</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to copy by configuring
    /// a <see cref="PropertyCopiers&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        TEntity payload,
        Func<PropertyCopiers<TEntity>, PropertyCopiers<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            [payload],
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Asynchronously and partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified
    /// by integer primary keys using an <see cref="IDbConnection"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of integer primary keys identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbConnection connection,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
        => BulkPartialUpdateAsync<TEntity, int>(connection, keys, propertySettersFactory, whereClause, commandParams);
    
    /// <summary>
    /// Asynchronously and partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified
    /// by integer primary keys using an <see cref="IDbTransaction"/>. Only the properties selected by the
    /// <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of integer primary keys identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity>(
        this IDbTransaction transaction,
        int[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
        => BulkPartialUpdateAsync<TEntity, int>(transaction, keys, propertySettersFactory, whereClause, commandParams);
    
    /// <summary>
    /// Asynchronously and partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified
    /// by keys of type <typeparamref name="TKey"/> using an <see cref="IDbConnection"/>. Only the properties selected
    /// by the <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="connection">A connection to the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity, TKey>(
        this IDbConnection connection,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            keys,
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return connection.ExecuteAsync(components.Command, components.Parameters);
    }
    
    /// <summary>
    /// Asynchronously and partially updates one or multiple entities of type <typeparamref name="TEntity"/> identified
    /// by keys of type <typeparamref name="TKey"/> using an <see cref="IDbTransaction"/>. Only the properties selected
    /// by the <paramref name="propertySettersFactory"/> are updated, and the update is constrained by the specified
    /// <paramref name="whereClause"/>. The updated values are configured through
    /// <paramref name="propertySettersFactory"/>.
    /// </summary>
    /// <typeparam name="TEntity">The type of the entities to update</typeparam>
    /// <typeparam name="TKey">The type of the primary key</typeparam>
    /// <param name="transaction">An open transaction in the database</param>
    /// <param name="keys">The array of primary key values identifying the entities to update</param>
    /// <param name="propertySettersFactory">
    /// A factory function that selects which properties to update by configuring
    /// a <see cref="PropertySetters&lt;TEntity&gt;"/> instance
    /// </param>
    /// <param name="whereClause">
    /// A SQL WHERE clause defining which entities to update (without the WHERE keyword)
    /// </param>
    /// <param name="commandParams">
    /// An object containing parameters for the SQL command. It is advisable to use
    /// <see cref="Dapper.DynamicParameters"/> here
    /// </param>
    /// <returns>The total number of affected rows</returns>
    public static Task<int> BulkPartialUpdateAsync<TEntity, TKey>(
        this IDbTransaction transaction,
        TKey[] keys,
        Func<PropertySetters<TEntity>, PropertySetters<TEntity>> propertySettersFactory,
        string whereClause,
        object commandParams = null!)
    {
        CommandBuilderData builderData = Cache.ResolveCommandBuilderData(typeof(TEntity));
        PartialUpdateComponents partials = propertySettersFactory(new())
            .GetPartialUpdateComponents(builderData);
        
        DynamicParameters initialParams = new();
        initialParams.AddDynamicParams(commandParams);
        
        CommandComponents components = GenerateUpdateSql(
            keys,
            Cache.ResolveCommandBuilderData(typeof(TEntity)),
            partials: partials,
            whereClause: whereClause,
            initialParams: initialParams);
        
        return transaction.ExecuteAsync(components.Command, components.Parameters);
    }
    
    #endregion
    private static CommandComponents GenerateUpdateSql<TValue>(
        TValue[] values,
        CommandBuilderData data,
        int enumerationStartIndex = 0,
        int enumerationEnd = 1,
        PartialUpdateComponents? partials = null,
        string? whereClause = null,
        DynamicParameters? initialParams = null)
    {
        StringBuilder commandBuilder = new();
        DynamicParameters parameters = initialParams ?? new(partials is null ? values.Length : partials.Updates.Length);
        bool hasCompositeKey = data.KeyProperties.Length > 1;
        
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
                        propertyWrapper,
                        hasCompositeKey);
                }
            }

            commandBuilder
                .Append(" WHERE ");

            if (whereClause is not null)
            {
                commandBuilder.Append(whereClause);
            }
            else
            {
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
        int initialLength = sb.Length;
        foreach (PropertyMapping property in properties)
        {
            string key = string.Concat("@", CompositionUtilities.ParameterPrefix, property.AssemblyName, index);
            
            if(!property.IsDbGenerated) parameters.Add(key, property.Getter(entity!));

            if (!property.IsKey)
            {
                sb.AppendWithSeparatorAndWrapper(property.DbName, propertyWrapper, ',', skipFirst);
                sb.Append("=").Append(key);
                skipFirst = false;
            }
        }

        if (initialLength == sb.Length)
            throw new CommandBuilderException(CommandBuilderExceptionType.InvalidUpdatePayload);
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
        int initialLength = sb.Length;
        foreach (PartialUpdates update in partials)
        {
            string key = string.Concat("@", CompositionUtilities.ParameterPrefix, update.Property.AssemblyName, index);
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
                if (!update.Property.IsDbGenerated)
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

        if (initialLength == sb.Length)
            throw new CommandBuilderException(CommandBuilderExceptionType.InvalidUpdatePayload);
    }
}