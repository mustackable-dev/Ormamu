using System.Reflection;
using Dapper;

namespace Ormamu;

internal class ConfigurableTypeMap(Type type, Func<string, string> nameConverter) : SqlMapper.ITypeMap
{
    private readonly SqlMapper.ITypeMap _innerMap = new CustomPropertyTypeMap(type,
        (t, columnName) => t.GetProperties().FirstOrDefault(
            prop => nameConverter(prop.Name) == columnName)!);

    public ConstructorInfo FindConstructor(string[] names, Type[] types) => _innerMap.FindConstructor(names, types)!;
    public SqlMapper.IMemberMap GetConstructorParameter(ConstructorInfo constructor, string columnName)
        => _innerMap.GetConstructorParameter(constructor, columnName)!;
    public SqlMapper.IMemberMap GetMember(string columnName) => _innerMap.GetMember(columnName)!;
    public ConstructorInfo FindExplicitConstructor() => _innerMap.FindExplicitConstructor()!;
}