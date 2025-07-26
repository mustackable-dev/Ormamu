namespace Ormamu;

/// <summary>
/// Provides configuration methods for setting up Ormamu.
/// </summary>
public static class Configuration
{
    /// <summary>
    /// <para>
    /// Configures Ormamu using a default <see cref="OrmamuOptions"/> instance. This default instance uses
    /// the <see cref="SqlDialect.SqlServer"/> dialect and does not apply any custom mapping between entity property
    /// names and SQL column names.
    /// </para>
    /// <para>
    /// This configuration will be applied globally to all entities.
    /// </para>
    /// </summary>
    public static void Apply()
        =>InitiateOrmamu([new()]);
    
    /// <summary>
    /// <para>
    /// Configures Ormamu with the specified instance of <see cref="OrmamuOptions"/>.
    /// </para>
    /// <para>
    /// This configuration applies to all entities.
    /// </para>
    /// </summary>
    /// <param name="options">The OrmamuOptions instance used for configuration.</param>
    public static void Apply(OrmamuOptions options)
        =>InitiateOrmamu([options]);
    
    
    /// <summary>
    /// <para>
    /// Configures Ormamu with multiple <see cref="OrmamuOptions"/> instances. Useful when your project uses
    /// multiple databases that use different naming conventions or dialects.
    /// </para>
    /// <para>
    /// Ensure each <see cref="OrmamuOptions"/> has a unique <see cref="OrmamuOptions.ConfigId"/>. Apply the
    /// <see cref="ConfigIdAttribute"/> to your entity classes to indicate which configuration to use.
    /// </para>
    /// </summary>
    /// <param name="configs">
    /// An array of <see cref="OrmamuOptions"/> instances, each representing a different configuration context.
    /// </param>
    public static void Apply(OrmamuOptions[] configs)
        =>InitiateOrmamu(configs);

    private static void InitiateOrmamu(OrmamuOptions[] configs)
    {
        Cache.GenerateCache(configs);
    }
}