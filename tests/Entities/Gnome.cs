using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Ormamu;

namespace OrmamuTests.Entities;

[ConfigId(TestsConfig.DbVariant)]
[Table(TestsConfig.UpdateTestsTableName, Schema = TestsConfig.SchemaName)]
public class Gnome
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public string Id { get; set; }
    public string Name { get; set; } = null!;
    public float Strength { get; set; }
    public int Height { get; set; }
    public bool IsActive { get; set; }
}