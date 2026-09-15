using System.Data;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;
using Xunit.Sdk;

namespace OrmamuTests;

public class UpsertTests
{
    private readonly string _nameColumn;
    private readonly DbFixture _fixture;

    public UpsertTests(DbFixture fixture)
    {
        if (TestsConfig.DbVariant == SqlDialect.SqlServer)
            throw SkipException.ForSkip("SQL Server is not supported for upserts");
        
        _fixture = fixture;
        _nameColumn = _fixture.DbProvider.Options.NameConverter("Name");
    }
    
    [Fact]
    public void Upsert_NewEntry_WithConnection_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };

        //Act
        int dwarfId = connection.Upsert(dwarf);
        Dwarf? dwarf2 = connection.Get<Dwarf>(dwarfId);

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(dwarf2 == dwarf);
    }

    [Fact]
    public void Upsert_ExistingEntry_WithConnection_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        int dwarfId = connection.Insert(dwarf);

        //Act
        dwarf.Id = dwarfId;
        dwarf.Name = "Buratin";
        dwarfId = connection.Upsert(dwarf);
        Dwarf? dwarf2 = connection.Get<Dwarf>(dwarfId);

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.Equal(dwarfId, dwarf.Id);
        Assert.Equal("Buratin", dwarf2.Name);
    }

    [Fact]
    public void Upsert_NewEntry_WithTransaction_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
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
        int dwarfId = transaction.Upsert(dwarf);
        Dwarf? dwarf2 = transaction.Get<Dwarf>(dwarfId);
        transaction.Commit();

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(dwarf2 == dwarf);
    }

    [Fact]
    public void Upsert_ExistingEntry_WithTransaction_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        int dwarfId = transaction.Insert(dwarf);

        //Act
        dwarf.Id = dwarfId;
        dwarf.Name = "Buratin1";
        dwarfId = transaction.Upsert(dwarf);
        Dwarf? dwarf2 = transaction.Get<Dwarf>(dwarfId);
        transaction.Commit();

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.Equal(dwarf.Id, dwarf2.Id);
        Assert.Equal("Buratin1", dwarf2.Name);
    }

    [Fact]
    public async Task UpsertAsync_NewEntry_WithConnection_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };

        //Act
        int dwarfId = await connection.UpsertAsync(dwarf);
        Dwarf? dwarf2 = await connection.GetAsync<Dwarf>(dwarfId);

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(dwarf2 == dwarf);
    }

    [Fact]
    public async Task UpsertAsync_ExistingEntry_WithConnection_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        int dwarfId = await connection.InsertAsync(dwarf);

        //Act
        dwarf.Id = dwarfId;
        dwarf.Name = "Buratin10";
        dwarfId = await connection.UpsertAsync(dwarf);
        Dwarf? dwarf2 = await connection.GetAsync<Dwarf>(dwarfId);

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.Equal(dwarf.Id, dwarf2.Id);
        Assert.Equal("Buratin10", dwarf2.Name);
    }

    [Fact]
    public async Task UpsertAsync_NewEntry_WithTransaction_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
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
        int dwarfId = await transaction.UpsertAsync(dwarf);
        Dwarf? dwarf2 = await transaction.GetAsync<Dwarf>(dwarfId);
        transaction.Commit();

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        dwarf2.Id = 0;
        Assert.True(dwarf2 == dwarf);
    }

    [Fact]
    public async Task UpsertAsync_ExistingEntry_WithTransaction_ShouldFindEntry()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Dwarf dwarf = new()
        {
            Name = "Buratino",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        int dwarfId = await transaction.InsertAsync(dwarf);

        //Act
        dwarf.Id = dwarfId;
        dwarf.Name = "Buratin11";
        dwarfId = await transaction.UpsertAsync(dwarf);
        Dwarf? dwarf2 = await transaction.GetAsync<Dwarf>(dwarfId);

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.Equal(dwarf.Id, dwarf2.Id);
        Assert.Equal("Buratin11", dwarf2.Name);
    }

    [Fact]
    public void Upsert_WithConnection_WithDbGeneratedProperty_ShouldReturnNull()
    {
        //Arrange

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Buratino37",
            Height = 120,
            IsActive = true,
            Strength = 50,
            HobbitAncestry = true
        };

        //Act
        int dwarfId = connection.Upsert(dwarf);
        Dwarf? dwarf2 = connection.Get<Dwarf>(dwarfId);

        //Assert
        Assert.True(dwarf2 is not null);
        Assert.True(dwarfId > 0);
        Assert.Null(dwarf2.HobbitAncestry);
    }

    [Fact]
    public void BulkUpsert_NewEntriesOnly_WithConnection_ShouldReturnCorrectCount()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        int dwarvesSampleSize = 278;

        Random random = new Random();
        Dwarf[] dwarves = new Dwarf[dwarvesSampleSize];
        string uniqueName = Guid.NewGuid().ToString();

        Array.Fill(
            dwarves,
            new()
            {
                Name = uniqueName,
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            });

        //Act
        int upsertedCount = connection.BulkUpsert(dwarves);
        IEnumerable<Dwarf> databaseDwarves = connection.Get<Dwarf>(
            whereClause: $"{_nameColumn}='{uniqueName}'");

        //Assert
        Assert.True(upsertedCount == dwarvesSampleSize);
        Assert.True(databaseDwarves.Count() == dwarvesSampleSize);
    }

    [Fact]
    public void BulkUpsert_NewAndExistingEntries_WithConnection_ShouldReturnCorrectCount()
    {
        //Arrange
        _fixture.DbProvider.WipeCreateTestsData();

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        int dwarvesSampleSize = 278;

        Random random = new Random();
        Dwarf[] dwarves = new Dwarf[dwarvesSampleSize];
        string uniqueName = Guid.NewGuid().ToString();

        Array.Fill(
            dwarves,
            new()
            {
                Name = uniqueName,
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            });

        Dwarf existingDwarf = new()
        {
            Name = "Harald"
        };

        Dwarf existingDwarf2 = new()
        {
            Name = "Marald"
        };

        existingDwarf.Id = connection.Insert(existingDwarf);
        existingDwarf2.Id = connection.Insert(existingDwarf2);

        existingDwarf.Name = "Harald1";
        existingDwarf2.Name = "Marald2";
        //Act
        int upsertedCount = connection.BulkUpsert([existingDwarf, ..dwarves, existingDwarf2]);
        IEnumerable<Dwarf> databaseDwarves = connection.Get<Dwarf>(
            whereClause: $"{_nameColumn}='{uniqueName}'");

        Dwarf? updateCheck = connection.Get<Dwarf>(existingDwarf.Id);
        //Assert
        Assert.True(upsertedCount == dwarvesSampleSize + 2);
        Assert.True(databaseDwarves.Count() == dwarvesSampleSize);
        Assert.NotNull(updateCheck);
        Assert.Equal("Harald1", updateCheck.Name);
    }

    [Fact]
    public void BulkUpsert_WithNewEntries_WithConnection_WithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        int gnomesSampleSize = 150;

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
        int upsertedCount = connection.BulkUpsert(gnomes);

        //Assert
        Assert.True(upsertedCount == gnomesSampleSize);
    }

    [Fact]
    public void BulkUpsert_WithNewAndExistingEntries_WithConnection_WithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange

        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        int gnomesSampleSize = 90;

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

        Gnome existingGnome = new()
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = "Jack",
            Height = random.Next(0, 120),
            IsActive = true,
            Strength = 50
        };

        connection.Insert<Gnome, string>(existingGnome);

        existingGnome.Strength = 49;

        //Act
        int upsertedCount = connection.BulkUpsert([..gnomes, existingGnome]);
        Gnome? existingGnomeCheck = connection.Get<Gnome, string>(existingGnome.Id);

        //Assert
        Assert.True(upsertedCount == gnomesSampleSize + 1);
        Assert.NotNull(existingGnomeCheck);
        Assert.Equal(49, existingGnomeCheck.Strength);
    }

    [Fact]
    public void BulkUpsert_WithNewEntries_WithTransaction_WithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        int gnomesSampleSize = 210;

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
        int upsertedCount = transaction.BulkUpsert(gnomes);
        transaction.Commit();

        //Assert
        Assert.True(upsertedCount == gnomesSampleSize);
    }

    [Fact]
    public void BulkUpsert_WithNewAndExistingEntries_WithTransaction_WithoutDbGeneratedKey_ShouldReturnCorrectCount()
    {
        //Arrange

        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        int gnomesSampleSize = 43;

        Random random = new Random();
        Gnome[] gnomes = new Gnome[gnomesSampleSize];

        for (int i = 0; i < gnomesSampleSize; i++)
        {
            gnomes[i] = new()
            {
                Id = Guid.NewGuid().ToString("n"),
                Name = "Buratino1",
                Height = random.Next(0, 120),
                IsActive = true,
                Strength = 50
            };
        }

        Gnome existingGnome = new()
        {
            Id = Guid.NewGuid().ToString("n"),
            Name = "John",
            Height = random.Next(0, 120),
            IsActive = true,
            Strength = 50
        };

        transaction.Insert<Gnome, string>(existingGnome);

        existingGnome.Strength = 48;

        //Act
        int upsertedCount = transaction.BulkUpsert([..gnomes, existingGnome]);
        Gnome? existingGnomeCheck = transaction.Get<Gnome, string>(existingGnome.Id);

        //Assert
        Assert.True(upsertedCount == gnomesSampleSize + 1);
        Assert.NotNull(existingGnomeCheck);
        Assert.Equal(48, existingGnomeCheck.Strength);
    }

    [Fact]
    public void Upsert_WithNewEntry_WithConnection_WithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 203,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        //Act
        ThrongletKey insertedKey = connection.Upsert<Thronglet, ThrongletKey>(thronglet);
        ThrongletKey key = new(Id: 203, Name: "Lemonadesther");
        Thronglet? thronglet2 = connection.Get<Thronglet, ThrongletKey>(key);
        //Assert
        Assert.True(insertedKey == key);
        Assert.NotNull(thronglet2);
        Assert.True(thronglet2 == thronglet);
    }

    [Fact]
    public void Upsert_WithExistingEntry_WithConnection_WithCompositeKey_ShouldFindEntry()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 201,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        ThrongletKey insertedKey = connection.Insert<Thronglet, ThrongletKey>(thronglet);
        //Act
        thronglet.Personality = Personality.Aloof;
        ThrongletKey upsertedKey = connection.Upsert<Thronglet, ThrongletKey>(thronglet);
        Thronglet? thronglet2 = connection.Get<Thronglet, ThrongletKey>(insertedKey);
        //Assert
        Assert.True(insertedKey == upsertedKey);
        Assert.NotNull(thronglet2);
        Assert.Equal(thronglet2.Personality, thronglet.Personality);
    }


    [Fact]
    public void Upsert_WithNewEntry_WithConnection_WithCompositeKey_WithAutoincrementingComponent_ShouldAssignNewId()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Gremlin gremlin =
            new()
            {
                Name = "Strawberryvonne",
                Personality = Personality.Assertive
            };
        
        Assert.SkipWhen(
            TestsConfig.DbVariant == SqlDialect.Sqlite,
            "SQLite does not support composite keys with an autoincrementing part, and as a result will " +
            "throw an error if an autoincrementing column is added in the ON CONFLICT clause. We cannot exclude " +
            "the autoincrementing element from this clause, because the uniqueness constraint will be less strict, " +
            "leading to cases where updates are carried out instead of inserts.");
        
        //Act
        GremlinKey insertedKey = connection.Upsert<Gremlin, GremlinKey>(gremlin);
        
        //Assert
        Assert.True(insertedKey.Id > 0);
    }


    [Fact]
    public void Upsert_WithExistingEntry_WithConnection_WithCompositeKey_WithAutoincrementingComponent_ShouldForceId()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();

        Gremlin gremlin =
            new()
            {
                Id = 20,
                Name = "Strawberryvonne",
                Personality = Personality.Assertive
            };
        
        Assert.SkipWhen(
            TestsConfig.DbVariant == SqlDialect.Sqlite,
            "SQLite does not support composite keys with an autoincrementing part, and as a result will " +
            "throw an error if an autoincrementing column is added in the ON CONFLICT clause. We cannot exclude " +
            "the autoincrementing element from this clause, because the uniqueness constraint will be less strict, " +
            "leading to cases where updates are carried out instead of inserts.");
        
        //Act
        GremlinKey insertedKey = connection.Upsert<Gremlin, GremlinKey>(gremlin);
        
        //Assert
        Assert.True(insertedKey.Id == 20);
    }

    [Fact]
    public void BulkUpsert_WithNewEntries_WithConnection_WithCompositeKey_ShouldUpsertCorrectCount()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        int sampleSize = 20;
        Thronglet[] thronglets = new Thronglet[sampleSize];
        for (int i = 0; i < thronglets.Length; i++)
        {
            thronglets[i] =
                new()
                {
                    Id = 10000 + i,
                    Name = "Cakemile",
                    Personality = Personality.Assertive
                };
        }

        //Act
        int upsertedCount = connection.BulkUpsert(thronglets);
        int namedCount = connection.Count<Thronglet, int>($"{_nameColumn}='Cakemile'");

        //Assert
        Assert.Equal(upsertedCount, sampleSize);
        Assert.Equal(namedCount, sampleSize);
    }

    [Fact]
    public void BulkUpsert_WithNewAndExistingEntries_WithConnection_WithCompositeKey_ShouldUpsertCorrectCount()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        int sampleSize = 35;
        Thronglet[] thronglets = new Thronglet[sampleSize];
        for (int i = 0; i < thronglets.Length; i++)
        {
            thronglets[i] =
                new()
                {
                    Id = 12000 + i,
                    Name = "Creaminnie",
                    Personality = Personality.Assertive
                };
        }

        Thronglet existingThronglet = new()
        {
            Id = 363636,
            Name = "George",
            Personality = Personality.Assertive
        };

        Thronglet existingThronglet2 = new()
        {
            Id = 363637,
            Name = "Porge",
            Personality = Personality.Aloof
        };

        connection.BulkInsert([existingThronglet, existingThronglet2]);

        existingThronglet.Personality = Personality.Driven;
        existingThronglet2.Personality = Personality.Reflective;

        //Act
        int upsertedCount = connection.BulkUpsert([existingThronglet, existingThronglet2, ..thronglets]);
        int namedCount = connection.Count<Thronglet, int>($"{_nameColumn}='Creaminnie'");

        Thronglet? updateCheck =
            connection.Get<Thronglet, ThrongletKey>(new ThrongletKey(existingThronglet2.Id, existingThronglet2.Name));

        //Assert
        Assert.Equal(upsertedCount, sampleSize + 2);
        Assert.Equal(namedCount, sampleSize);
        Assert.NotNull(updateCheck);
        Assert.Equal(Personality.Reflective, existingThronglet2.Personality);
    }

    [Fact]
    public void
        BulkUpsert_WithNewEntries_WithConnection_WithCompositeKey_WithAutoincrementingComponent_ShouldUpsertCorrectCount()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        int sampleSize = 20;
        Gremlin[] gremlins = new Gremlin[sampleSize];
        for (int i = 0; i < gremlins.Length; i++)
        {
            gremlins[i] =
                new()
                {
                    Name = "Cakemile",
                    Personality = Personality.Assertive
                };
        }
        
        Assert.SkipWhen(
            TestsConfig.DbVariant == SqlDialect.Sqlite,
            "SQLite does not support composite keys with an autoincrementing part, and as a result will " +
            "throw an error if an autoincrementing column is added in the ON CONFLICT clause. We cannot exclude " +
            "the autoincrementing element from this clause, because the uniqueness constraint will be less strict, " +
            "leading to cases where updates are carried out instead of inserts.");
        
        //Act
        int upsertedCount = connection.BulkUpsert(gremlins);
        int namedCount = connection.Count<Gremlin, int>($"{_nameColumn}='Cakemile'");
        //Assert
        Assert.Equal(upsertedCount, sampleSize);
        Assert.Equal(namedCount, sampleSize);
    }

    [Fact]
    public void
        BulkUpsert_WithNewAndExistingEntries_WithConnection_WithCompositeKey_WithAutoincrementingComponent_ShouldUpsertCorrectCount()
    {
        //Arrange
        using IDbConnection connection = _fixture.DbProvider.GetConnection();
        int sampleSize = 35;
        Gremlin[] thronglets = new Gremlin[sampleSize];
        for (int i = 0; i < thronglets.Length; i++)
        {
            thronglets[i] =
                new()
                {
                    Name = "Creaminnie",
                    Personality = Personality.Assertive
                };
        }

        Gremlin existingGremlin = new()
        {
            Id = 363636,
            Name = "George",
            Personality = Personality.Assertive
        };

        Gremlin existingGremlin2 = new()
        {
            Id = 363637,
            Name = "Porge",
            Personality = Personality.Aloof
        };

        Assert.SkipWhen(
            TestsConfig.DbVariant == SqlDialect.Sqlite,
            "SQLite does not support composite keys with an autoincrementing part, and as a result will " +
            "throw an error if an autoincrementing column is added in the ON CONFLICT clause. We cannot exclude " +
            "the autoincrementing element from this clause, because the uniqueness constraint will be less strict, " +
            "leading to cases where updates are carried out instead of inserts.");

        connection.BulkInsert([existingGremlin, existingGremlin2]);

        existingGremlin.Personality = Personality.Driven;
        existingGremlin2.Personality = Personality.Reflective;

        //Act
        int upsertedCount = connection.BulkUpsert([existingGremlin, ..thronglets, existingGremlin2]);
        int namedCount = connection.Count<Gremlin, int>($"{_nameColumn}='Creaminnie'");

        Gremlin? updateCheck =
            connection.Get<Gremlin, GremlinKey>(new GremlinKey(existingGremlin2.Id, existingGremlin2.Name));

        //Assert
        Assert.Equal(upsertedCount, sampleSize + 2);
        Assert.Equal(namedCount, sampleSize);
        Assert.NotNull(updateCheck);
        Assert.Equal(Personality.Reflective, existingGremlin2.Personality);
    }
}