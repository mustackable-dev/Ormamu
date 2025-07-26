namespace Ormamu;

internal readonly struct KeyParam<T>
{
    public T KeyValue { get; }

    internal KeyParam(T keyValue) => KeyValue = keyValue;
}