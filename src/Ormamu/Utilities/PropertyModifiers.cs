using System.Linq.Expressions;

namespace Ormamu;

/// <summary>
/// Represents a collection of property setters for a specific entity type <typeparamref name="TEntity"/>.
/// Provides functionality to define property updates for partial updates.
/// </summary>
/// <typeparam name="TEntity">The entity type whose properties are being updated</typeparam>
public class PropertySetters<TEntity> : PropertyModifiers
{
    /// <summary>
    /// Defines a property to be updated with a specified value when building partial updates
    /// </summary>
    /// <typeparam name="TProperty">The property type selected on the entity</typeparam>
    /// <param name="selector">An expression selecting the property on <typeparamref name="TEntity"/></param>
    /// <param name="value">The value to assign to the selected property</param>
    /// <returns>The current <see cref="PropertySetters{TEntity}"/> instance</returns>
    public PropertySetters<TEntity> SetProperty<TProperty>(
        Expression<Func<TEntity, TProperty>> selector,
        TProperty value)
    {
        Setters.Add(new Setter(selector, value));
        return this;
    }
    internal PartialUpdateComponents GetPartialUpdateComponents(CommandBuilderData builderData)
        => GetPartialUpdateComponents(builderData, false);
}

/// <summary>
/// Represents a collection of property copiers for a specific entity type <typeparamref name="TEntity"/>.
/// Provides functionality to define property copy operations for partial updates.
/// </summary>
/// <typeparam name="TEntity">The entity type whose properties are being copied</typeparam>
public class PropertyCopiers<TEntity> : PropertyModifiers
{
    /// <summary>
    /// Defines a property to be copied from a <typeparamref name="TEntity"/> instance
    /// </summary>
    /// <typeparam name="TProperty">The type of the property being copied</typeparam>
    /// <param name="selector">An expression selecting the property on <typeparamref name="TEntity"/></param>
    /// <returns>The current <see cref="PropertyCopiers{TEntity}"/> instance</returns>
    public PropertyCopiers<TEntity> CopyProperty<TProperty>(
        Expression<Func<TEntity, TProperty>> selector) 
    {
        Setters.Add(new Setter(selector, null));
        return this;
    }
    internal PartialUpdateComponents GetPartialUpdateComponents(CommandBuilderData builderData)
        => GetPartialUpdateComponents(builderData, true);
}

/// <summary>
/// Base class for property modifiers, providing common functionality for property setters and copiers.
/// Tracks the set of property changes and provides partial update generation.
/// </summary>
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

    internal PartialUpdateComponents GetPartialUpdateComponents(
        CommandBuilderData builderData,
        bool copy)
    {
        PartialUpdates[] keyUpdates = builderData.KeyProperties
            .Select(x=> new PartialUpdates(x, null, true))
            .ToArray();
        
        return new(keyUpdates.Concat(GetPartialUpdates((builderData))).ToArray(), copy);
    }
    internal sealed record Setter(Expression Selector, object? Value);
}

internal sealed record PartialUpdateComponents(PartialUpdates[] Updates, bool Copy);
internal sealed record PartialUpdates(PropertyMapping Property, object? Value, bool KeySetter = false);