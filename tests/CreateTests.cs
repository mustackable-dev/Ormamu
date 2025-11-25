using System.Data;
using System.Text.Json;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class CreateTests(DbFixture fixture)
{
    private readonly string _nameColumn = fixture.DbProvider.Options.NameConverter("Name");
    
    [Fact]
    public void Insert_WithConnection_ShouldFindEntry()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = connection.Insert(dwarf);
        Dwarf? dwarf2 = connection.Get<Dwarf>(dwarfId);
        
        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(JsonSerializer.Serialize(dwarf2) == JsonSerializer.Serialize(dwarf));
    }
    
    [Fact]
    public void Insert_WithTransaction_ShouldFindEntry()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = transaction.Insert(dwarf);
        Dwarf? dwarf2 = transaction.Get<Dwarf>(dwarfId);
        transaction.Commit();
        
        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(JsonSerializer.Serialize(dwarf2) == JsonSerializer.Serialize(dwarf));
    }
    
    [Fact]
    public async Task InsertAsync_WithConnection_ShouldFindEntry()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = await connection.InsertAsync(dwarf);
        Dwarf? dwarf2 = await connection.GetAsync<Dwarf>(dwarfId);
        
        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(JsonSerializer.Serialize(dwarf2) == JsonSerializer.Serialize(dwarf));
    }
    
    [Fact]
    public async Task InsertAsync_WithTransaction_ShouldFindEntry()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = await transaction.InsertAsync(dwarf);
        Dwarf? dwarf2 = await transaction.GetAsync<Dwarf>(dwarfId);
        transaction.Commit();
        
        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(JsonSerializer.Serialize(dwarf2) == JsonSerializer.Serialize(dwarf));
    }
    
    [Fact]
    public void BulkInsert_WithConnection_ShouldReturnCorrectCount()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int dwarvesSampleSize = 300;
        
        Random random = new Random();
        Dwarf[] dwarves = new Dwarf[dwarvesSampleSize];
        string uniqueName = Guid.NewGuid().ToString();
        
        Array.Fill(dwarves, new()
        {
            Name = uniqueName,
            Height = random.Next(0, 120),
            IsActive = true,
            Strength = 50
        });
        
        //Act
        int insertedCount = connection.BulkInsert(dwarves);
        IEnumerable<Dwarf> databaseDwarves = connection.Get<Dwarf>(
            whereClause: $"{_nameColumn}='{uniqueName}'");
        
        //Assert
        Assert.True(insertedCount == dwarvesSampleSize);
        Assert.True(databaseDwarves.Count() == dwarvesSampleSize);
    }
    
    [Fact]
    public void BulkInsert_WithTransaction_ShouldReturnCorrectCount()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        int dwarvesSampleSize = 300;
        
        Random random = new Random();
        Dwarf[] dwarves = new Dwarf[dwarvesSampleSize];

        for (int i = 0; i < dwarvesSampleSize; i++)
        {
            dwarves[i] = new()
            {
                Name = "Buratino",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }
        
        //Act
        int insertedCount = transaction.BulkInsert(dwarves);
        IEnumerable<Dwarf> databaseDwarves = transaction.Get<Dwarf>();
        transaction.Commit();
        
        //Assert
        Assert.True(insertedCount == dwarvesSampleSize);
        Assert.True(databaseDwarves.Count() == dwarvesSampleSize);
    }
    
    [Fact]
    public async Task BulkInsertAsync_WithConnection_ShouldReturnCorrectCount()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int dwarvesSampleSize = 300;
        
        Random random = new Random();
        Dwarf[] dwarves = new Dwarf[dwarvesSampleSize];

        for (int i = 0; i < dwarvesSampleSize; i++)
        {
            dwarves[i] = new()
            {
                Name = "Buratino",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }
        
        //Act
        int insertedCount = await connection.BulkInsertAsync(dwarves);
        IEnumerable<Dwarf> databaseDwarves = await connection.GetAsync<Dwarf>();
        
        //Assert
        Assert.True(insertedCount == dwarvesSampleSize);
        Assert.True(databaseDwarves.Count() == dwarvesSampleSize);
    }
    
    [Fact]
    public async Task BulkInsertAsync_WithTransaction_ShouldReturnCorrectCount()
    {
        //Arrange
        fixture.DbProvider.WipeCreateTestsData();
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        int dwarvesSampleSize = 300;
        string uniqueName = Guid.NewGuid().ToString();
        
        Random random = new Random();
        Dwarf[] dwarves = new Dwarf[dwarvesSampleSize];
        
        Array.Fill(dwarves, new()
        {
            Name = uniqueName,
            Height = random.Next(0, 120),
            IsActive = true,
            Strength = 50
        });
        
        //Act
        int insertedCount = await transaction.BulkInsertAsync(dwarves);
        IEnumerable<Dwarf> databaseDwarves = await transaction.GetAsync<Dwarf>(
            whereClause: $"{_nameColumn}='{uniqueName}'");
        transaction.Commit();
        
        //Assert
        Assert.True(insertedCount == dwarvesSampleSize);
        Assert.True(databaseDwarves.Count() == dwarvesSampleSize);
    }
    [Fact]
    public void Insert_WithConnectionWithDbGeneratedProperty_ShouldReturnNull()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino1",
            Height = 120,
            IsActive = true,
            Strength = 50,
            HobbitAncestry = true
        };
        
        //Act
        int dwarfId = connection.Insert(dwarf);
        Dwarf? dwarf2 = connection.Get<Dwarf>(dwarfId);
        
        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        Assert.Null(dwarf2.HobbitAncestry);
    }
    
    [Fact]
    public void BulkInsert_WithConnectionWithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        
        int gnomesSampleSize = 300;
        
        Random random = new Random();
        Gnome[] gnomes = new Gnome[gnomesSampleSize];

        for (int i = 0; i < gnomesSampleSize; i++)
        {
            gnomes[i] = new()
            {
                Id = Guid.NewGuid().ToString("n"),
                Name = "Buratino",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }
        
        //Act
        int insertedCount = connection.BulkInsert(gnomes);
        
        //Assert
        Assert.True(insertedCount == gnomesSampleSize);
    }
    
    [Fact]
    public void BulkInsert_WithTransactionWithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        int gnomesSampleSize = 300;
        
        Random random = new Random();
        Gnome[] gnomes = new Gnome[gnomesSampleSize];

        for (int i = 0; i < gnomesSampleSize; i++)
        {
            gnomes[i] = new()
            {
                Id = Guid.NewGuid().ToString("n"),
                Name = "Buratino",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }
        
        //Act
        int insertedCount = transaction.BulkInsert(gnomes);
        transaction.Commit();
        
        //Assert
        Assert.True(insertedCount == gnomesSampleSize);
    }
    
    [Fact]
    public async Task BulkInsertAsync_WithConnectionWithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        
        int gnomesSampleSize = 300;
        
        Random random = new Random();
        Gnome[] gnomes = new Gnome[gnomesSampleSize];

        for (int i = 0; i < gnomesSampleSize; i++)
        {
            gnomes[i] = new()
            {
                Id = Guid.NewGuid().ToString("n"),
                Name = "Buratino",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }
        
        //Act
        int insertedCount = await connection.BulkInsertAsync(gnomes);
        
        //Assert
        Assert.True(insertedCount == gnomesSampleSize);
    }
    
    [Fact]
    public async Task BulkInsertAsync_WithTransactionWithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        int gnomesSampleSize = 300;
        
        Random random = new Random();
        Gnome[] gnomes = new Gnome[gnomesSampleSize];

        for (int i = 0; i < gnomesSampleSize; i++)
        {
            gnomes[i] = new()
            {
                Id = Guid.NewGuid().ToString("n"),
                Name = "Buratino",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }
        
        //Act
        int insertedCount = await transaction.BulkInsertAsync(gnomes);
        transaction.Commit();
        
        //Assert
        Assert.True(insertedCount == gnomesSampleSize);
    }
    
    [Fact]
    public void Insert_WithConnectionOnMulticonfig_ShouldFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.MulticonfigDbProvider.GetConnection();

        Imp imp = new()
        {
            GuidKey = Guid.NewGuid(),
            Name = "Puratino",
            FavouriteLetter = 'a',
            Age = 12,
        };
        
        //Act
        
        Guid impId = connection.Insert<Imp, Guid>(imp);
        Imp? imp2 = connection.Get<Imp, Guid>(impId);
        
        //Assert
        
        Assert.True(imp2 is not null);
    }
    
    [Fact]
    public void BulkInsert_WithConnectionOnMulticonfig_ShouldFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.MulticonfigDbProvider.GetConnection();
        
        int impsSampleSize = 300;
        string name = "Schnitzel";
        
        Imp[] imps = new Imp[impsSampleSize];

        for (int i = 0; i < impsSampleSize; i++)
        {
            imps[i] = new()
            {
                GuidKey = Guid.NewGuid(),
                Name = name,
                FavouriteLetter = 'a',
                Age = 12,
            };
        }
        
        //Act
        
        int insertedImps = connection.BulkInsert(imps, 30);
        
        //Assert
        
        Assert.True(insertedImps == impsSampleSize);
        IEnumerable<Imp> imps2 = connection.Get<Imp>($"\"{TestsConfig.CustomColumnName}\"=@templateName", commandParams: new { templateName = name });
        Assert.True(imps2 is not null);
        Assert.True(imps2.Count() == impsSampleSize);
    }
    
    [Fact]
    public async Task InsertAsync_WithConnectionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 18,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        //Act
        ThrongletKey insertedKey = await connection.InsertAsync<Thronglet, ThrongletKey>(thronglet);
        ThrongletKey key = new(Id: 18, Name: "Lemonadesther");
        Thronglet? thronglet2 = await connection.GetAsync<Thronglet, ThrongletKey>(key);
        //Assert
        Assert.True(insertedKey == key);
        Assert.NotNull(thronglet2);
        Assert.True(JsonSerializer.Serialize(thronglet2) == JsonSerializer.Serialize(thronglet));
    }
    
    [Fact]
    public async Task InsertAsync_WithTransactionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet =
            new()
            {
                Id = 17,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        //Act
        ThrongletKey insertedKey = await transaction.InsertAsync<Thronglet, ThrongletKey>(thronglet);
        ThrongletKey key = new(Id: 17, Name: "Lemonadesther");
        Thronglet? thronglet2 = await transaction.GetAsync<Thronglet, ThrongletKey>(key);
        transaction.Commit();
        //Assert
        Assert.True(insertedKey == key);
        Assert.NotNull(thronglet2);
        Assert.True(JsonSerializer.Serialize(thronglet2) == JsonSerializer.Serialize(thronglet));
    }
    
    [Fact]
    public void Insert_WithConnectionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 20,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        //Act
        ThrongletKey insertedKey = connection.Insert<Thronglet, ThrongletKey>(thronglet);
        ThrongletKey key = new(Id: 20, Name: "Lemonadesther");
        Thronglet? thronglet2 = connection.Get<Thronglet, ThrongletKey>(key);
        //Assert
        Assert.True(insertedKey == key);
        Assert.NotNull(thronglet2);
        Assert.True(JsonSerializer.Serialize(thronglet2) == JsonSerializer.Serialize(thronglet));
    }
    
    [Fact]
    public void Insert_WithTransactionWithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet =
            new()
            {
                Id = 19,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        
        //Act
        ThrongletKey insertedKey = transaction.Insert<Thronglet, ThrongletKey>(thronglet);
        ThrongletKey key = new(Id: 19, Name: "Lemonadesther");
        Thronglet? thronglet2 = transaction.Get<Thronglet, ThrongletKey>(key);
        transaction.Commit();
        //Assert
        Assert.True(insertedKey == key);
        Assert.NotNull(thronglet2);
        Assert.True(JsonSerializer.Serialize(thronglet2) == JsonSerializer.Serialize(thronglet));
    }
    
    [Fact]
    public void Insert_WithConnectionWithCompositeKeyWithAutoincrementingComponent_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gremlin gremlin =
            new()
            {
                Id = 20,
                Name = "Strawberryvonne",
                Personality = Personality.Assertive
            };
        //Act
        GremlinKey insertedKey = connection.Insert<Gremlin, GremlinKey>(gremlin);
        //Assert
        Assert.True(insertedKey.Id != 20);
    }
    
    [Fact]
    public void Insert_WithTransactionWithCompositeKeyWithAutoincrementingComponent_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gremlin gremlin =
            new()
            {
                Id = 11,
                Name = "Apricotom",
                Personality = Personality.Assertive
            };
        //Act
        GremlinKey insertedKey = transaction.Insert<Gremlin, GremlinKey>(gremlin);
        transaction.Commit();
        //Assert
        Assert.True(insertedKey.Id != 11);
    }
}