using System.ComponentModel.DataAnnotations.Schema;

namespace OrmamuTests.Entities;

[Table(TestsConfig.DeleteTestsTableName, Schema = TestsConfig.SchemaName)]
public class Pixie: EntityBase<long>
{
    public long MagicPower { get; set; }
    public DateTime DateOfBirth { get; set; }
    [NotMapped]
    public string Ignored { get; set; }
}