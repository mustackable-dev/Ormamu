namespace Ormamu;
internal sealed record CommandBuilderData(
    OrmamuBaseOptions Options,
    string DbIdentifier,
    string ColumnsString,
    PropertyMapping PrimaryKey,
    PropertyMapping[] Properties,
    CompositeKeyData? CompositeKeyData = null);
internal sealed record PropertyMapping(
    string DbName,
    string AssemblyName,
    bool IsKey,
    bool IsDbGenerated,
    Func<object, object> Getter);

internal sealed record CompositeKeyData(
    Type KeyType,
    PropertyMapping[] Properties);