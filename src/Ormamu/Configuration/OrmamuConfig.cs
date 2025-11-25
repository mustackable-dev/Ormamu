namespace Ormamu;

/// <summary>
/// Provides configuration methods for setting up Ormamu.
/// </summary>
public static class OrmamuConfig
{
    internal static void Apply()
        =>InitiateOrmamu(new (){ {true, new()} });
    
    /// <summary>
    /// <para>
    /// Configures Ormamu with the specified instance of <see cref="OrmamuOptions"/>.
    /// </para>
    /// <para>
    /// This configuration applies to all entities.
    /// </para>
    /// </summary>
    /// <param name="options">The OrmamuOptions instance used for configuration</param>
    public static void Apply(OrmamuOptions options)
        =>InitiateOrmamu(new (){ {true, options} });
    
    
    /// <summary>
    /// <para>
    /// Configures Ormamu with multiple <see cref="OrmamuOptions"/> instances. Useful when your project uses
    /// multiple databases that use different naming conventions or dialects.
    /// </para>
    /// </summary>
    /// <param name="configs">
    /// A configuration mapping specifying <see cref="OrmamuOptions"/> to be used for each
    /// <see cref="OrmamuConfigIdAttribute"/> tag
    /// </param>
    public static void Apply(Dictionary<object, OrmamuOptions> configs)
        =>InitiateOrmamu(configs);

    private static void InitiateOrmamu(Dictionary<object, OrmamuOptions> configs)
    {
        Cache.GenerateCache(configs);
    }
}