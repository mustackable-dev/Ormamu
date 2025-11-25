using Ormamu;
using Ormamu.Exceptions;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class ConfigurationTests(DbFixture fixture)
{
    [Fact]
    public void Apply_WithNonExistentKey_ShouldThrowException()
        => Assert.Throws<CacheBuilderException>(()=>
            OrmamuConfig.Apply(new Dictionary<object, OrmamuOptions>
            {
                {"FakeKey", new OrmamuOptions()}
            })
        );
    [Fact]
    public void Get_WithConnectionOnNonRegisteredClass_ShouldThrowException()
        => Assert.Throws<CommandBuilderException>(()=>
            fixture.DbProvider.GetConnection().Get<DateTime>(1)
        );
}