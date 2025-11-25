using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Ormamu;

namespace OrmamuTests.Entities;

public record struct GremlinKey(int Id, string Name);

[CompositeKey(typeof(GremlinKey))]
[Table(TestsConfig.AutoIncrementingCompositeKeyTestsTableName, Schema = TestsConfig.SchemaName)]
[OrmamuConfigId(TestsConfig.DbVariant)]
public class Gremlin
{
    [Key]
    public int Id { get; set; }
    
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public string Name { get; set; } = null!;
    public Personality Personality { get; set; }
}