using System.ComponentModel.DataAnnotations.Schema;
using Ormamu;

namespace OrmamuTests.Entities;

public record struct ThrongletKey(int Id, string Name);

[CompositeKey(typeof(ThrongletKey))]
[Table(TestsConfig.CompositeKeyTestsTableName, Schema = TestsConfig.SchemaName)]
[ConfigId(TestsConfig.DbVariant)]
public class Thronglet
{
    public int Id { get; set; }
    public string Name { get; set; } = null!;
    public Personality Personality { get; set; }
}

public enum Personality
{
    Driven,
    Aloof,
    Reflective,
    Introverted,
    Assertive
}