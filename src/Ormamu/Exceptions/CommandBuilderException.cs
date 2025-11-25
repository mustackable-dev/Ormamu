namespace Ormamu.Exceptions;

/// <summary>
/// A class for all exceptions thrown during the command build phase
/// </summary>
public class CommandBuilderException: Exception
{
    /// <summary>
    /// The type of exception that was thrown
    /// </summary>
    public CommandBuilderExceptionType ExceptionType { get; }

    private static readonly Dictionary<CommandBuilderExceptionType, string> ErrorMessageTemplates = new()
    {
        {
            CommandBuilderExceptionType.MissingClass, "The class specified for this operation was not flagged for " +
                                                    "querying. Please make sure the class is decorated with the " +
                                                    "[Table] attribute and has a [Key] attribute on a property."
        },
        {
            CommandBuilderExceptionType.CompositeKeyTypeNotRegistered, "The composite key type \"{0}\" used for this " +
                                                                      "operation was not registered with the " +
                                                                      "[CompositeKey] attribute for this entity"
        },
        {
            CommandBuilderExceptionType.InvalidUpdatePayload, "The update payload is invalid or empty. Primary keys " +
                                                              "(or single components of primary keys) cannot be updated"
        }
    };
    
    internal CommandBuilderException(CommandBuilderExceptionType type, params object?[] args) :
        base($"{type.ToString()} - {string.Format(ErrorMessageTemplates[type], args)}")
    {
        ExceptionType = type;
    }
}