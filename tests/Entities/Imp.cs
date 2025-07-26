using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Ormamu;

namespace OrmamuTests.Entities;

[ConfigId(TestsConfig.MulticonfigTestsTableName)]
[Table(TestsConfig.MulticonfigTestsTableName, Schema = TestsConfig.SecondarySchemaName)]
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