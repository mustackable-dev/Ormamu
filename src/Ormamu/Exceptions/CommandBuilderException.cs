namespace Ormamu.Exceptions;

/// <summary>
/// A class for all exceptions thrown during the query build phase
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
        }
    };
    
    internal CommandBuilderException(CommandBuilderExceptionType type, params object?[] args) :
        base($"{type.ToString()} - {string.Format(ErrorMessageTemplates[type], args)}")
    {
        ExceptionType = type;
    }
}