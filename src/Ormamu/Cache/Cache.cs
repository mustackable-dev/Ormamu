using System.Collections.Frozen;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Dapper;
using Ormamu.Exceptions;

namespace Ormamu;

internal static class Cache
{
    private static readonly object ConfigLock = new();
    private static FrozenDictionary<Type, CommandBuilderData>? BuilderDataCache { get; set; }

    internal static void GenerateCache(Dictionary<object, OrmamuOptions> buildOptions)
    {
        Dictionary<Type, CommandBuilderData> cache = new();
        foreach (var type in AppDomain.CurrentDomain.GetAssemblies().SelectMany(x => x.GetTypes()))
        {
            TableAttribute? tableAttribute = type.GetCustomAttribute<TableAttribute>();
            if(tableAttribute is null) continue;
            
            OrmamuConfigIdAttribute? configAttribute = type.GetCustomAttribute<OrmamuConfigIdAttribute>();
            OrmamuOptions options = buildOptions.First().Value;
            if (configAttribute is not null && !buildOptions.TryGetValue(configAttribute.ConfigId, out options!))
            {
                throw new CacheBuilderException(CacheBuilderExceptionType.ConfigNotFound, configAttribute.ConfigId, type.Name);
            }
            
            List<PropertyMapping> properties = new();
            List<PropertyMapping> keyProperties = new();
            Dictionary<string, string> customPropertyNameDictionary = new();
            
            ExtractProperties(properties, keyProperties, customPropertyNameDictionary, type, options);
            
            CompositeKeyAttribute? compositeKeyAttribute = type.GetCustomAttribute<CompositeKeyAttribute>();
            
            if (compositeKeyAttribute is not null)
            {
                keyProperties = ConvertToCompositeKeyProperties(compositeKeyAttribute, keyProperties, options, type);
            }
            
            if (keyProperties.Count == 0) continue;
            
            string paddingSymbol = options.Dialect switch
            {
                SqlDialect.PostgreSql => "\"",
                SqlDialect.MySql or SqlDialect.MariaDb => "`",
                _=> ""
            };
            string columnsString = String.Join(",", properties
                .Where(x => !x.IsDbGenerated)
                .Select(x => $"{paddingSymbol}{x.DbName}{paddingSymbol}"));
            
            cache.TryAdd(
                type,
                new(
                    options,
                    tableAttribute.GetDbObjectIdentifier(options.Dialect, paddingSymbol),
                    columnsString,
                    keyProperties.ToArray(),
                    properties.ToArray()
                )
            );
            
            Func<string, string> typeMapConverter = options.NameConverter;
            if (customPropertyNameDictionary.Count > 0)
            {
                FrozenDictionary<string, string> lookUpDictionary = customPropertyNameDictionary.ToFrozenDictionary();
                typeMapConverter = x =>
                {
                    if (lookUpDictionary.TryGetValue(x, out string? value))
                    {
                        return value;
                    }

                    return options.NameConverter(x);
                };
            }
            SqlMapper.SetTypeMap(type, new ConfigurableTypeMap(type, typeMapConverter));
        }
        BuilderDataCache = cache.ToFrozenDictionary();
    }

    private static void ExtractProperties(
        List<PropertyMapping> properties,
        List<PropertyMapping> keyProperties,
        Dictionary<string, string> customPropertyNameDictionary,
        Type type,
        OrmamuOptions options)
    {
        foreach (PropertyInfo property in type.GetProperties(options.PropertyBindingFlags))
        {
            PropertyData? data = GetPropertyData(property);
                
            if (data is null) continue;
            if(data.HasCustomName) customPropertyNameDictionary.Add(data.AssemblyName, data.Name);
                
            PropertyMapping mapping = new(
                data.HasCustomName ? data.Name : options.NameConverter(data.Name),
                data.AssemblyName,
                data.IsKey,
                data.IsDbGenerated,
                GenerateGetter(type, property));
                
            if (data.IsKey) keyProperties.Add(mapping);
            properties.Add(mapping);
        }
    }

    private static List<PropertyMapping> ConvertToCompositeKeyProperties(
        CompositeKeyAttribute compositeKeyAttribute,
        List<PropertyMapping> keyProperties,
        OrmamuOptions options,
        Type type)
    {
        PropertyInfo[] compositeKeyProperties = compositeKeyAttribute.KeyType.GetProperties(options.PropertyBindingFlags);
        return keyProperties.Select(x =>
        {
            PropertyInfo? property = compositeKeyProperties.FirstOrDefault(y => x.AssemblyName == y.Name);
            if (property is null)
                throw new CacheBuilderException(
                    CacheBuilderExceptionType.CompositeKeyMissingProperty,
                    compositeKeyAttribute.KeyType.Name,
                    type.Name,
                    x.AssemblyName);
            return x with
            {
                CompositeKeyGetter = GenerateGetter(
                    compositeKeyAttribute.KeyType,
                    property
                )
            };
        }).ToList();
    }

    private static string GetDbObjectIdentifier(this TableAttribute attribute, SqlDialect dialect, string paddingSymbol)
    {
        
        StringBuilder sb = new(paddingSymbol);
        if (
            attribute.Schema is not null &&
            !Array.Exists([SqlDialect.MySql, SqlDialect.MariaDb, SqlDialect.Sqlite], x=>x==dialect))
        {
            sb
                .Append(attribute.Schema)
                .Append(paddingSymbol)
                .Append(".")
                .Append(paddingSymbol);
        }
        sb.Append(attribute.Name).Append(paddingSymbol);
        
        return sb.ToString();
    }

    private static PropertyData? GetPropertyData(PropertyInfo property)
    {
        if(property.PropertyType.IsValueType || property.PropertyType == typeof(string))
        {
            bool isKey = false;
            bool isDbGenerated = false;
            
            if(property.GetCustomAttribute<NotMappedAttribute>() is not null) return null;
            KeyAttribute? keyAttribute = property.GetCustomAttribute<KeyAttribute>();
            
            if (keyAttribute is not null)
            {
                isKey = true;
                isDbGenerated = true;
            }
            
            DatabaseGeneratedAttribute? dbGeneratedAttribute = property.GetCustomAttribute<DatabaseGeneratedAttribute>();

            if (dbGeneratedAttribute is not null)
            {
                isDbGenerated = dbGeneratedAttribute.DatabaseGeneratedOption != DatabaseGeneratedOption.None;
            }
            
            ColumnAttribute? columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
            if (columnAttribute is not null && !string.IsNullOrEmpty(columnAttribute.Name))
            {
                return new(columnAttribute.Name, property.Name, isKey, isDbGenerated, true);
            }
            return new(property.Name, property.Name, isKey, isDbGenerated);
        }

        return null;
    }

    internal static CommandBuilderData ResolveCommandBuilderData(Type identifier)
    {
        if (BuilderDataCache is null)
        {
            lock(ConfigLock)
            {
                if(BuilderDataCache is null)
                    OrmamuConfig.Apply();
            }
        }
        
        if (BuilderDataCache!.TryGetValue(identifier, out CommandBuilderData? entry))
        {
            return entry;
        }
        
        throw new CommandBuilderException(CommandBuilderExceptionType.MissingClass, identifier.Name);
    }
    private static Func<object, object> GenerateGetter(Type baseClass, PropertyInfo property)
    {
        Type objectType = typeof(object);
        ParameterExpression parameterExpression = Expression.Parameter(objectType, "x");
        UnaryExpression memberExpression = Expression.Convert(
                                                Expression.Property(
                                                    Expression.Convert(parameterExpression, baseClass),
                                                    property),
                                                objectType);
        return Expression.Lambda<Func<object, object>>(memberExpression, parameterExpression).Compile();
    }
    private sealed record PropertyData(
        string Name,
        string AssemblyName,
        bool IsKey,
        bool IsDbGenerated,
        bool HasCustomName = false);
}