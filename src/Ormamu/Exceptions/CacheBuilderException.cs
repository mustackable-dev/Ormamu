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
        }
    };
    
    internal CacheBuilderException(CacheBuilderExceptionType type, params object?[] args) :
        base($"{type.ToString()} - {string.Format(ErrorMessageTemplates[type], args)}")
    {
        ExceptionType = type;
    }
}