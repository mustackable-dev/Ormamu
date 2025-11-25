using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Ormamu;

namespace OrmamuTests.Entities;

[OrmamuConfigId(TestsConfig.MultiConfigTestsTableName)]
[Table(TestsConfig.MultiConfigTestsTableName, Schema = TestsConfig.SecondarySchemaName)]
public class Imp
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public Guid GuidKey { get; set; }
    
    [Column(TestsConfig.CustomColumnName)]
    public string Name { get; set; } = null!;
    public char FavouriteLetter { get; set; }
    public short Age { get; set; }
}