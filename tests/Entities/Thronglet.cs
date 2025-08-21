using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Ormamu;

namespace OrmamuTests.Entities;

public record struct ThrongletKey(int Id, string Name);

[Table(TestsConfig.CompositeKeyTestsTableName, Schema = TestsConfig.SchemaName)]
[ConfigId(TestsConfig.DbVariant)]
public class Thronglet
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public int Id { get; set; }
    
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
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