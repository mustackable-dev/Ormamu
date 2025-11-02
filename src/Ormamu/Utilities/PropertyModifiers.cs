using System.Linq.Expressions;

namespace Ormamu;

public class PropertySetters<TEntity> : PropertyModifiers
{
    public PropertySetters<TEntity> SetProperty<TProperty, TValue>(
        Expression<Func<TEntity, TProperty>> selector,
        TValue value)
    {
        Setters.Add(new Setter(selector, value));
        return this;
    }
}

public class PropertyCopiers<TEntity> : PropertyModifiers
{
    public PropertyCopiers<TEntity> CopyProperty<TProperty>(
        Expression<Func<TEntity, TProperty>> selector) 
    {
        Setters.Add(new Setter(selector, null));
        return this;
    }
}
public abstract class PropertyModifiers
{
    internal readonly List<Setter> Setters = new();
    private PartialUpdates[] GetPartialUpdates(CommandBuilderData builderData)
    {
        List<PartialUpdates> components = new();
        foreach (Setter setter in Setters)
        {
            string propertyName = setter.Selector.ParsePropertyName();
            components.Add(new(
                builderData.Properties.First(x=>x.AssemblyName==propertyName),
                setter.Value));
        }
        return components.ToArray();
    }

    internal PartialUpdateComponents GetPartialUpdateComponents<TKey>(
        CommandBuilderData builderData,
        TKey? entityKey)
    {
        PartialUpdates[] keyUpdates = builderData.KeyProperties
            .Select(x=>
            {
                object? value = entityKey;
                if (value is not null)
                {
                    value = builderData.KeyProperties.Length > 1 ? x.CompositeKeyGetter!(entityKey!) : entityKey;
                }
                return new PartialUpdates(x, value);
            }).ToArray();
        return new(keyUpdates.Concat(GetPartialUpdates((builderData))).ToArray(), entityKey is null);
    }
    internal sealed record Setter(Expression Selector, object? Value);
}

internal sealed record PartialUpdateComponents(PartialUpdates[] Updates, bool Copy);
internal sealed record PartialUpdates(PropertyMapping Property, object? Value);