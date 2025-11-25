namespace Ormamu;
internal sealed record CommandBuilderData(
    OrmamuOptions Options,
    string DbIdentifier,
    string ColumnsString,
    PropertyMapping[] KeyProperties,
    PropertyMapping[] Properties);
internal sealed record PropertyMapping(
    string DbName,
    string AssemblyName,
    bool IsKey,
    bool IsDbGenerated,
    Func<object, object> Getter,
    Func<object, object>? CompositeKeyGetter = null);