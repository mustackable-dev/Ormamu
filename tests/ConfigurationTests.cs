using Ormamu;
using Ormamu.Exceptions;

namespace OrmamuTests;

public class ConfigurationTests
{
    [Fact]
    public void Apply_WithNonExistentKey_ShouldThrowException()
        => Assert.Throws<CacheBuilderException>(()=>
            OrmamuConfig.Apply(new Dictionary<object, OrmamuOptions>
            {
                {"FakeKey", new OrmamuOptions()}
            })
        );
}