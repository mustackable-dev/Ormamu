namespace Ormamu.Exceptions;

/// <summary>
/// Defines the possible exceptions that can be raised during the cache build process
/// </summary>
public enum CacheBuilderExceptionType
{
    /// <summary>
    /// A composite key property from the entity model is missing in the registered composite key type
    /// </summary>
    CompositeKeyMissingProperty,
    /// <summary>
    /// An invalid type was used in a ConfigId attribute
    /// </summary>
    InvalidConfigIdType,
    ConfigNotFound,
}