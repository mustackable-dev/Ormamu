using System.ComponentModel.DataAnnotations;
using Ormamu;

namespace OrmamuTests.Entities;

[ConfigId(TestsConfig.DbVariant)]
public abstract class EntityBase<T>
{
    [Key]
    public T Id { get; set; }
}
