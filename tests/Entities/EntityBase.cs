using System.ComponentModel.DataAnnotations;
using Ormamu;

namespace OrmamuTests.Entities;

[OrmamuConfigId(TestsConfig.DbVariant)]
public abstract record EntityBase<T>
{
    [Key]
    public T Id { get; set; }
}
