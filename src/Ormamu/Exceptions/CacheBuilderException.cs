namespace Ormamu.Exceptions;

/// <summary>
/// A class for all exceptions thrown during the cache build phase
/// </summary>
public class CacheBuilderException: Exception
{
    /// <summary>
    /// The type of exception that was thrown
    /// </summary>
    public CacheBuilderExceptionType ExceptionType { get; }

    private static readonly Dictionary<CacheBuilderExceptionType, string> ErrorMessageTemplates = new()
    {
        {
            CacheBuilderExceptionType.CompositeKeyMissingProperty, "The composite key type \"{0}\" registered for " +
                                                                       "\"{1}\" is missing the key property \"{2}\""
        },
        {
            CacheBuilderExceptionType.InvalidConfigIdType, "The provided value \"{0}\" of type \"{1}\", used in a " +
                                                           "ConfigId attribute is not a value type, you can only use " +
                                                           "value types for config ids"
        },
        {
            CacheBuilderExceptionType.ConfigNotFound, "Failed to find a configuration with id \"{0}\", which was " +
                                                       "specified for entity \"{1}\". Please make sure you have " +
                                                       "added a configuration with this id in your " +
                                                       "Ormamu.Configuration.Apply method"
        }
    };
    
    internal CacheBuilderException(CacheBuilderExceptionType type, params object?[] args) :
        base($"{type.ToString()} - {string.Format(ErrorMessageTemplates[type], args)}")
    {
        ExceptionType = type;
    }
}