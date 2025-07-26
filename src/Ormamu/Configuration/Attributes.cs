namespace Ormamu;

/// <summary>
/// Apply this attribute to an entity definition to designate which <see cref="OrmamuOptions"/> configuration
/// should be used.
/// </summary>
/// <param name="configId">The <see cref="OrmamuOptions.ConfigId"/> of the <see cref="OrmamuOptions"/>
/// to apply</param>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct)]
public class ConfigIdAttribute(object configId) : Attribute
{
    internal object ConfigId { get; private set; } = configId;
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