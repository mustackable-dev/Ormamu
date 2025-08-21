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
    private static FrozenDictionary<Type, CommandBuilderData> BuilderDataCache { get; set; } = null!;

    internal static void GenerateCache(OrmamuOptions[] buildOptions)
    {
        Dictionary<Type, CommandBuilderData> cache = new();
        foreach (var type in AppDomain.CurrentDomain.GetAssemblies().SelectMany(x => x.GetTypes()))
        {
            TableAttribute? tableAttribute = type.GetCustomAttribute<TableAttribute>();
            if(tableAttribute is null) continue;
            
            ConfigIdAttribute? configAttribute = type.GetCustomAttribute<ConfigIdAttribute>();
            OrmamuOptions options =
                (configAttribute is null ?
                    null :
                    buildOptions.FirstOrDefault(x => Equals(x.ConfigId, configAttribute.ConfigId))) ?? buildOptions[0];
            
            List<PropertyMapping> properties = new();
            List<PropertyMapping> keyProperties = new();
            
            Dictionary<string, string> customPropertyNameDictionary = new();
            foreach (var property in type.GetProperties(options.PropertyBindingFlags))
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
                    tableAttribute.GetDbObjectIdentifier(options),
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

    private static string GetDbObjectIdentifier(this TableAttribute attribute, OrmamuOptions options)
    {
            
        string paddingSymbol = options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _=> ""
        };
        
        StringBuilder sb = new(paddingSymbol);
        if (
            attribute.Schema is not null &&
            !Array.Exists([SqlDialect.MySql, SqlDialect.MariaDb, SqlDialect.Sqlite], x=>x==options.Dialect))
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
        if (BuilderDataCache.TryGetValue(identifier, out CommandBuilderData? entry))
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