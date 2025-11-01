using Ormamu.Exceptions;

namespace Ormamu;


/// <summary>
/// Apply this attribute to an entity definition to designate which <see cref="OrmamuOptions"/> configuration
/// should be used. Only ValueTypes and strings are allowed as config ids!
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class ConfigIdAttribute : Attribute
{
    internal object ConfigId { get; private set; }

    /// <summary>
    /// Apply this attribute to an entity definition to designate which <see cref="OrmamuOptions"/> configuration
    /// should be used. Only ValueTypes and strings are allowed as config ids!
    /// </summary>
    /// <param name="configId">The <see cref="OrmamuOptions.ConfigId"/> of the <see cref="OrmamuOptions"/>
    /// to apply</param>
    public ConfigIdAttribute(object configId)
    {
        Type idType = configId.GetType();
        if(!idType.IsValueType && idType != typeof(string))
            throw new CacheBuilderException(CacheBuilderExceptionType.InvalidConfigIdType, configId, idType.Name);
        
        ConfigId = configId;
    }
}

/// <summary>
/// Apply this attribute to an entity definition to specify the type of the composite key used by the entity.
/// Make sure all the property names defined in the composite key type match the property names in the entity.
/// </summary>
/// <param name="keyType">A type definition that matches your composite key structure.</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class CompositeKeyAttribute(Type keyType) : Attribute
{
    internal Type KeyType { get; private set; } = keyType;
}