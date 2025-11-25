using System.ComponentModel.DataAnnotations.Schema;

namespace OrmamuTests.Entities;

[Table(TestsConfig.ReadTestsTableName, Schema = TestsConfig.SchemaName)]
public record Goblin: EntityBase<int>
{
    public string Name { get; set; } = null!;
    public char FavouriteLetter { get; set; }
    public short Age { get; set; }
    public byte Stamina { get; set; }
    public long MagicPower { get; set; }
    public float Strength { get; set; }
    public double Agility { get; set; }
    public decimal Salary { get; set; }
    public bool IsActive { get; set; }
    public DateTime DateOfBirth { get; set; }
    public bool? HobbitAncestry { get; set; } = false;
}