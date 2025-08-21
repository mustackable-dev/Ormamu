namespace Ormamu.Exceptions;

/// <summary>
/// Defines the possible exceptions that can be raised during the query build process
/// </summary>
public enum CommandBuilderExceptionType
{
    /// <summary>
    /// Class has not been flagged for queries
    /// </summary>
    MissingClass,
    /// <summary>
    /// A composite key type has not been registered for an entity
    /// </summary>
    CompositeKeyTypeNotRegistered,
}