using System.Reflection;

namespace Ormamu;

/// <summary>
/// Defines a configuration to be used during query generation with Ormamu. If you use multiple databases with
/// different naming conventions, use the <see cref="ConfigId"/> attribute to specify which configuration
/// should be applied to a given entity.
/// </summary>
public record OrmamuOptions: OrmamuBaseOptions
{
    /// <summary>
    /// Determines the property binding flags used for extracting properties from entities.
    /// </summary>
    public BindingFlags PropertyBindingFlags { get; set; } = BindingFlags.Public | BindingFlags.Instance;
}


/// <summary>
/// This is the base definition for <see cref="OrmamuOptions"/>.
/// </summary>
public record OrmamuBaseOptions
{
    
    /// <summary>
    /// The <see cref="SqlDialect"/> to use for generating queries.
    /// </summary>
    public SqlDialect Dialect { get; set; }
    
    /// <summary>
    /// Here you can define a custom name converter function that will be used to map entity property names
    /// to database column names. For your convenience, we populated the <see cref="NameConverters"/> class
    /// with some popular conversions like snake case and kebab case.
    /// </summary>
    public Func<string, string> NameConverter { get; set; } = x => x;
}