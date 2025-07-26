using System.Data;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class ReadTests(DbFixture fixture)
{
    private readonly string _idColumn = fixture.DbProvider.Options.NameConverter("Id");
    private readonly string _ageColumn = fixture.DbProvider.Options.NameConverter("Age");
    private readonly string _isActiveColumn = fixture.DbProvider.Options.NameConverter("IsActive");
    private readonly string _agilityColumn = fixture.DbProvider.Options.NameConverter("Agility");
    
    [Fact]
    public void Get_SingleWithConnectionWithIntKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int tenthId = 10;
        //Act
        Goblin? goblin = connection.Get<Goblin>(tenthId);
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public async Task GetAsync_SingleWithConnectionWithIntKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int tenthId = 10;
        //Act
        Goblin? goblin = await connection.GetAsync<Goblin>(tenthId);
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public void Get_SingleWithConnectionWithTypedKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int tenthId = 10;
        //Act
        Goblin? goblin = connection.Get<int, Goblin>(tenthId);
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public async Task GetAsync_SingleWithConnectionWithTypedKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int tenthId = 10;
        //Act
        Goblin? goblin = await connection.GetAsync<int, Goblin>(tenthId);
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public void Get_SingleWithTransactionWithIntKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        int tenthId = 10;
        //Act
        Goblin? goblin = transaction.Get<Goblin>(tenthId);
        
        transaction.Commit();
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public async Task GetAsync_SingleWithTransactionWithIntKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        int tenthId = 10;
        //Act
        Goblin? goblin = await transaction.GetAsync<Goblin>(tenthId);
        
        transaction.Commit();
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public void Get_SingleWithTransactionWithTypedKey_ShouldFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        int tenthId = 10;
        
        //Act
        
        Goblin? goblin = transaction.Get<int, Goblin>(tenthId);
        
        transaction.Commit();
        
        //Assert
        
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public async Task GetAsync_SingleWithTransactionWithTypedKey_ShouldFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        int tenthId = 10;
        
        //Act
        
        Goblin? goblin = await transaction.GetAsync<int, Goblin>(tenthId);
        
        transaction.Commit();
        
        //Assert
        Assert.NotNull(goblin);
        Assert.True(goblin.Id == tenthId);
    }
    
    [Fact]
    public void Get_MultipleWithConnection_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        
        //Act
        IEnumerable<Goblin> goblins = connection.Get<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            param: new { Id = 10, Age = 30 });
            
        //Assert
        Assert.True(goblins.Count() == 5);
        Assert.True(goblins.First().Id == 25);
    }
    
    [Fact]
    public void Get_MultipleWithTransaction_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        
        //Act
        
        IEnumerable<Goblin> goblins = transaction.Get<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            param: new { Id = 10, Age = 30 });
        
        transaction.Commit();
            
        //Assert
        
        Assert.True(goblins.Count() == 5);
        Assert.True(goblins.First().Id == 25);
    }
    
    [Fact]
    public async Task GetAsync_MultipleWithConnection_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        
        //Act
        IEnumerable<Goblin> goblins = await connection.GetAsync<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            param: new { Id = 10, Age = 30 });
            
        //Assert
        Assert.True(goblins.Count() == 5);
        Assert.True(goblins.First().Id == 25);
    }
    
    [Fact]
    public async Task GetAsync_MultipleWithTransaction_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        
        //Act
        IEnumerable<Goblin> goblins = await transaction.GetAsync<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            param: new { Id = 10, Age = 30 });
        
        transaction.Commit();
            
        //Assert
        Assert.True(goblins.Count() == 5);
        Assert.True(goblins.First().Id == 25);
    }
    
    [Fact]
    public void Get_MultipleWithConnectionWithPaging_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        int pageSize = 2;
        int pageNumber = 2;
        
        //Act
        IEnumerable<Goblin> goblins = connection.Get<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            pageSize,
            pageNumber,
            param: new { Id = 10, Age = 30 });
            
        //Assert
        Assert.True(goblins.Count() == 2);
        Assert.True(goblins.First().Id == 19);
    }
    
    [Fact]
    public void Get_MultipleWithTransactionWithPaging_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        int pageSize = 2;
        int pageNumber = 1;
        
        //Act
        IEnumerable<Goblin> goblins = transaction.Get<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            pageSize,
            pageNumber,
            param: new { Id = 10, Age = 30 });
            
        transaction.Commit();
        
        //Assert
        Assert.True(goblins.Count() == 2);
        Assert.True(goblins.First().Id == 25);
    }
    
    [Fact]
    public async Task GetAsync_MultipleWithConnectionWithPaging_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        int pageSize = 3;
        int pageNumber = 2;
        
        //Act
        IEnumerable<Goblin> goblins = await connection.GetAsync<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            pageSize,
            pageNumber,
            param: new { Id = 10, Age = 30 });
            
        //Assert
        Assert.True(goblins.Count() == 2);
        Assert.True(goblins.First().Id == 28);
    }
    
    [Fact]
    public async Task GetAsync_MultipleWithTransactionWithPaging_ShouldReturnCorrectCountAndFirst()
    {
        //Arrange
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
            
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        string where = $"{wrapper}{_idColumn}{wrapper} > @Id AND {wrapper}{_ageColumn}{wrapper} < @Age";
        string orderBy = $"{wrapper}{_isActiveColumn}{wrapper} ASC, {wrapper}{_agilityColumn}{wrapper} DESC";
        int pageSize = 3;
        int pageNumber = 1;
        
        //Act
        IEnumerable<Goblin> goblins = await transaction.GetAsync<Goblin>(
            whereClause: where,
            orderByClause: orderBy,
            pageSize,
            pageNumber,
            param: new { Id = 10, Age = 30 });
        
        transaction.Commit();
            
        //Assert
        Assert.True(goblins.Count() == 3);
        Assert.True(goblins.First().Id == 25);
    }
    
    [Fact]
    public void Get_SingleWithConnectionOnMulticonfig_ShouldFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.MulticonfigDbProvider.GetConnection();

        Imp[] imps =
        [
            new()
            {
                GuidKey = Guid.NewGuid(),
                Name = "Puratino",
                FavouriteLetter = 'a',
                Age = 12,
            },
            new()
            {
                GuidKey = Guid.NewGuid(),
                Name = "Auratino",
                FavouriteLetter = 'b',
                Age = 19,
            }
        ];
        
        foreach (Imp imp in imps)
        {
            imp.GuidKey = connection.Insert<Guid, Imp>(imp);
        }
        
        //Act
        Imp? imp2 = connection.Get<Guid, Imp>(imps[1].GuidKey);
        
        //Assert
        
        Assert.True(imp2 is not null);
    }
    
    [Fact]
    public void Get_SingleWithConnectionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 1,
                Name = "Hamburgerry",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 2,
                Name = "Fixit",
                Personality = Personality.Driven
            },
            new()
            {
                Id = 3,
                Name = "Bananadege",
                Personality = Personality.Introverted
            }
        ];
        
        connection.BulkInsert(thronglets);
        //Act
        ThrongletKey key = new(Id: 2, Name: "Fixit");
        Thronglet? thronglet = connection.Get<ThrongletKey, Thronglet>(key);
        //Assert
        Assert.NotNull(thronglet);
        Assert.True(key.Equals(new ThrongletKey(Id: thronglet.Id, Name: thronglet.Name)));
    }
    
    [Fact]
    public void Get_SingleAsyncWithTransactionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 4,
                Name = "Sandwichandler",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 5,
                Name = "Propit",
                Personality = Personality.Driven
            },
            new()
            {
                Id = 6,
                Name = "Tinyomi",
                Personality = Personality.Introverted
            }
        ];
        
        transaction.BulkInsert(thronglets);
        //Act
        ThrongletKey key = new(Id: 6, Name: "Tinyomi");
        Thronglet? thronglet = transaction.Get<ThrongletKey, Thronglet>(key);
        
        transaction.Commit();
        //Assert
        Assert.NotNull(thronglet);
        Assert.True(key.Equals(new ThrongletKey(Id: thronglet.Id, Name: thronglet.Name)));
    }
    
    [Fact]
    public async Task GetAsync_SingleWithConnectionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 7,
                Name = "Hamburgerry",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 8,
                Name = "Fixit",
                Personality = Personality.Driven
            },
            new()
            {
                Id = 9,
                Name = "Bananadege",
                Personality = Personality.Introverted
            }
        ];
        
        await connection.BulkInsertAsync(thronglets);
        //Act
        ThrongletKey key = new(Id: 8, Name: "Fixit");
        Thronglet? thronglet = await connection.GetAsync<ThrongletKey, Thronglet>(key);
        //Assert
        Assert.NotNull(thronglet);
        Assert.True(key.Equals(new ThrongletKey(Id: thronglet.Id, Name: thronglet.Name)));
    }
    
    [Fact]
    public async Task GetAsync_SingleWithTransactionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 10,
                Name = "Sandwichandler",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 11,
                Name = "Propit",
                Personality = Personality.Driven
            },
            new()
            {
                Id = 12,
                Name = "Tinyomi",
                Personality = Personality.Introverted
            }
        ];
        
        await transaction.BulkInsertAsync(thronglets);
        //Act
        ThrongletKey key = new(Id: 12, Name: "Tinyomi");
        Thronglet? thronglet = await transaction.GetAsync<ThrongletKey, Thronglet>(key);
        
        transaction.Commit();
        //Assert
        Assert.NotNull(thronglet);
        Assert.True(key.Equals(new ThrongletKey(Id: thronglet.Id, Name: thronglet.Name)));
    }
}