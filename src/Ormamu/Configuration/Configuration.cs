namespace Ormamu;

/// <summary>
/// Provides configuration methods for setting up Ormamu.
/// </summary>
public static class Configuration
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
    /// <param name="options">The OrmamuOptions instance used for configuration.</param>
    public static void Apply(OrmamuOptions options)
        =>InitiateOrmamu(new (){ {true, options} });
    
    
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
    public static void Apply(Dictionary<object, OrmamuOptions> configs)
        =>InitiateOrmamu(configs);

    private static void InitiateOrmamu(Dictionary<object, OrmamuOptions> configs)
    {
        Cache.GenerateCache(configs);
    }
}