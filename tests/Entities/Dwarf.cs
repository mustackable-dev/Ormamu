using System.ComponentModel.DataAnnotations.Schema;

namespace OrmamuTests.Entities;

[Table(TestsConfig.CreateTestsTableName, Schema = TestsConfig.SchemaName)]
public record Dwarf: EntityBase<int>
{
    public string Name { get; set; } = null!;
    public float Strength { get; set; }
    public int Height { get; set; }
    public bool IsActive { get; set; }
    
    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
    public bool? HobbitAncestry { get; set; }
}