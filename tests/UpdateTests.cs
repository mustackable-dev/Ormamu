using System.Data;
using System.Text.Json;
using Dapper;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class UpdateTests(DbFixture fixture)
{
    private readonly string _heightColumn = fixture.DbProvider.Options.NameConverter("Height");
    private readonly string _idColumn = fixture.DbProvider.Options.NameConverter("Id");
    private readonly string _nameColumn = fixture.DbProvider.Options.NameConverter("Name");
    
    [Fact]
    public void Update_WithConnection_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Tubulin",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        string gnomeId = connection.Insert<Gnome, string>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Height = 130;
        int updatedRecords = connection.Update(gnome);
        Gnome? gnomeFromDb = connection.Get<Gnome, string>(gnomeId);
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb.Height == 130);
    }
    
    [Fact]
    public void Update_WithTransaction_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Tublin",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        string gnomeId = transaction.Insert<Gnome, string>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Height = 130;
        int updatedRecords = transaction.Update(gnome);
        Gnome? gnomeFromDb = transaction.Get<Gnome, string>(gnomeId);
        transaction.Commit();
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb.Height == 130);
    }
    
    [Fact]
    public async Task UpdateAsync_WithConnection_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Tubulin",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        string gnomeId = await connection.InsertAsync<Gnome, string>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Name = "Mazelin";
        int updatedRecords = await connection.UpdateAsync(gnome);
        Gnome? gnomeFromDb = await connection.GetAsync<Gnome, string>(gnomeId);
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb.Name == "Mazelin");
    }
    
    [Fact]
    public async Task UpdateAsync_WithTransaction_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Tubulin",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        string gnomeId = await transaction.InsertAsync<Gnome, string>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Name = "Sni";
        int updatedRecords = await transaction.UpdateAsync(gnome);
        Gnome? gnomeFromDb = await transaction.GetAsync<Gnome, string>(gnomeId);
        transaction.Commit();
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb.Name == "Sni");
    }

    [Fact]
    public void BulkUpdate_WithConnection_ShouldHaveChangedValues()
    {
        //Arrange
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int height = 9995;
        string title = "Guliver";

        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubulin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubu1lin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubu1lin",
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        foreach (Gnome gnome in gnomes)
        {
            gnome.Id=connection.Insert<Gnome, string>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = connection.BulkUpdate(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = connection.Get<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", commandParams: new {height});
        
        //Assert
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(gnomesFromDb.All(x=>x.Name == title));
    }

    [Fact]
    public void BulkUpdate_WithTransaction_ShouldHaveChangedValues()
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
        
        int height = 300;
        string title = "Andre";

        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubulin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubu1lin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tbu1lin",
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        foreach (Gnome gnome in gnomes)
        {
            gnome.Id=transaction.Insert<Gnome, string>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = transaction.BulkUpdate(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = transaction.Get<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", commandParams: new {height});
        transaction.Commit();
        
        //Assert
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(gnomesFromDb.All(x=>x.Name == title));
    }

    [Fact]
    public async Task BulkUpdateAsync_WithConnection_ShouldHaveChangedValues()
    {
        //Arrange
        
        
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        int height = 444;
        string title = "Kane";

        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubulin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubu1lin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tbu1lin",
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        foreach (Gnome gnome in gnomes)
        {
            gnome.Id= await connection.InsertAsync<Gnome, string>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = await connection.BulkUpdateAsync(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = await connection.GetAsync<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", commandParams: new {height});
        
        //Assert
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(gnomesFromDb.All(x=>x.Name == title));
    }

    [Fact]
    public async Task BulkUpdateAsync_WithTransaction_ShouldHaveChangedValues()
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
        
        int height = 380;
        string title = "Marco";

        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubulin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tubu1lin",
                Height = height,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = "Tbu1lin",
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        foreach (Gnome gnome in gnomes)
        {
            gnome.Id= await transaction.InsertAsync<Gnome, string>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        DynamicParameters queryParameters = new();
        queryParameters.AddDynamicParams(new { height });
        
        int updatedRecords = await transaction.BulkUpdateAsync(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = await transaction.GetAsync<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", commandParams: queryParameters);
        transaction.Commit();
        
        //Assert
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(gnomesFromDb.All(x=>x.Name == title));
    }
    
    [Fact]
    public void Update_WithConnectionOnMulticonfig_ShouldHaveChangedValue()
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
        imp.Name = "MegaPuratino";
        int updatedCount = connection.Update(imp);
        Imp? imp3 = connection.Get<Imp, Guid>(impId);
        
        //Assert
        
        Assert.True(updatedCount == 1);
        Assert.True(imp2 is not null);
        Assert.True(imp2.Name == "Puratino");
        Assert.True(imp3 is not null);
        Assert.True(imp3.Name == "MegaPuratino");
    }
    
    [Fact]
    public void BulkUpdate_WithConnectionOnMulticonfig_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.MulticonfigDbProvider.GetConnection();
        
        int impsSampleSize = 110;
        string name = "Hawaii";
        
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
        
        int insertedImps = connection.BulkInsert(imps, 99);
        IEnumerable<Imp> imps2 = connection.Get<Imp>($"\"{TestsConfig.CustomColumnName}\"=@templateName", commandParams: new { templateName = name });
        imps = imps.Select(x =>
        {
            x.Name = "Milano";
            return x;
        }).ToArray();
        int updatedImps = connection.BulkUpdate(imps);
        IEnumerable<Imp> imps3 = connection.Get<Imp>($"\"{TestsConfig.CustomColumnName}\"=@templateName", commandParams: new { templateName = "Milano" });
        //Assert
        
        Assert.True(insertedImps == impsSampleSize);
        Assert.True(imps2 is not null);
        Assert.True(imps2.Count() == impsSampleSize);
        Assert.True(updatedImps == impsSampleSize);
        Assert.True(imps3.All(x => x.Name == "Milano"));
    }
    
    [Fact]
    public void Update_WithConnectionWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 33,
                Name = "Smoothierica",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = connection.Insert<Thronglet, ThrongletKey>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = connection.Update(thronglet);
        Thronglet? thronglet2 = connection.Get<Thronglet, ThrongletKey>(insertedKey);
        //Assert
        Assert.True(updatedCount == 1);
        Assert.NotNull(thronglet2);
        Assert.True(thronglet2.Personality == Personality.Reflective);
    }
    
    [Fact]
    public void Update_WithTransactionWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet =
            new()
            {
                Id = 34,
                Name = "Smoothierica",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = transaction.Insert<Thronglet, ThrongletKey>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = transaction.Update(thronglet);
        Thronglet? thronglet2 = transaction.Get<Thronglet, ThrongletKey>(insertedKey);
        transaction.Commit();
        //Assert
        Assert.True(updatedCount == 1);
        Assert.NotNull(thronglet2);
        Assert.True(thronglet2.Personality == Personality.Reflective);
    }
    
    [Fact]
    public async Task UpdateAsync_WithConnectionWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 35,
                Name = "Smoothierica",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = await connection.InsertAsync<Thronglet, ThrongletKey>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = await connection.UpdateAsync(thronglet);
        Thronglet? thronglet2 = await connection.GetAsync<Thronglet, ThrongletKey>(insertedKey);
        //Assert
        Assert.True(updatedCount == 1);
        Assert.NotNull(thronglet2);
        Assert.True(thronglet2.Personality == Personality.Reflective);
    }
    
    [Fact]
    public async Task UpdateAsync_WithTransactionWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet =
            new()
            {
                Id = 36,
                Name = "Smoothierica",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = await transaction.InsertAsync<Thronglet, ThrongletKey>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = await transaction.UpdateAsync(thronglet);
        Thronglet? thronglet2 = await transaction.GetAsync<Thronglet, ThrongletKey>(insertedKey);
        transaction.Commit();
        //Assert
        Assert.True(updatedCount == 1);
        Assert.NotNull(thronglet2);
        Assert.True(thronglet2.Personality == Personality.Reflective);
    }

    [Fact]
    public void BulkUpdate_WithConnectionWithCompositeKey_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 37,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 38,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 39,
                Name = "Sushivette",
                Personality = Personality.Assertive
            }
        ];
        
        connection.BulkInsert(thronglets);
        IEnumerable<Thronglet> thronglets2 = connection.Get<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: new { id = 37, id2 = 39 });
        
        //Act
        
        int updatedThronglets = connection.BulkUpdate(thronglets.Select(x =>
        {
            x.Personality = Personality.Reflective;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = connection.Get<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: new { id = 37, id2 = 39 });
        
        //Assert
        
        Assert.True(updatedThronglets == 3);
        Assert.True(thronglets2.Count()==3);
        Assert.True(thronglets2.Select(x=>x.Id).SequenceEqual(thronglets3.Select(x=>x.Id)));
        Assert.False(thronglets2.Select(x=>x.Personality).SequenceEqual(thronglets3.Select(x=>x.Personality)));
    }

    [Fact]
    public void BulkUpdate_WithTransactionWithCompositeKey_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 40,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 41,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 42,
                Name = "Sushivette",
                Personality = Personality.Assertive
            }
        ];
        
        transaction.BulkInsert(thronglets);
        IEnumerable<Thronglet> thronglets2 = transaction.Get<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: new { id = 40, id2 = 42 });
        
        //Act
        
        int updatedThronglets = transaction.BulkUpdate(thronglets.Select(x =>
        {
            x.Personality = Personality.Aloof;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = transaction.Get<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: new { id = 40, id2 = 42 });
        
        transaction.Commit();
        
        //Assert
        
        Assert.True(updatedThronglets == 3);
        Assert.True(thronglets2.Count()==3);
        Assert.True(thronglets2.Select(x=>x.Id).SequenceEqual(thronglets3.Select(x=>x.Id)));
        Assert.False(thronglets2.Select(x=>x.Personality).SequenceEqual(thronglets3.Select(x=>x.Personality)));
    }

    [Fact]
    public async Task BulkUpdateAsync_WithConnectionWithCompositeKey_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 43,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 44,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 45,
                Name = "Sushivette",
                Personality = Personality.Assertive
            }
        ];
        
        await connection.BulkInsertAsync(thronglets);
        IEnumerable<Thronglet> thronglets2 = await connection.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: new { id = 43, id2 = 45 });
        
        //Act
        
        int updatedThronglets = await connection.BulkUpdateAsync(thronglets.Select(x =>
        {
            x.Personality = Personality.Reflective;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = await connection.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: new { id = 43, id2 = 45 });
        
        //Assert
        
        Assert.True(updatedThronglets == 3);
        Assert.True(thronglets2.Count()==3);
        Assert.True(thronglets2.Select(x=>x.Id).SequenceEqual(thronglets3.Select(x=>x.Id)));
        Assert.False(thronglets2.Select(x=>x.Personality).SequenceEqual(thronglets3.Select(x=>x.Personality)));
    }

    [Fact]
    public async Task BulkUpdateAsync_WithTransactionWithCompositeKey_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 46,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 47,
                Name = "Sushivette",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 48,
                Name = "Sushivette",
                Personality = Personality.Assertive
            }
        ];
        
        await transaction.BulkInsertAsync(thronglets);
        
        DynamicParameters queryParameters = new();
        queryParameters.AddDynamicParams(new { id = 46, id2 = 48 });
        
        IEnumerable<Thronglet> thronglets2 = await transaction.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: queryParameters);
        
        //Act
        
        int updatedThronglets = await transaction.BulkUpdateAsync(thronglets.Select(x =>
        {
            x.Personality = Personality.Aloof;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = await transaction.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            commandParams: queryParameters);

        transaction.Commit();
        
        //Assert
        
        Assert.True(updatedThronglets == 3);
        Assert.True(thronglets2.Count()==3);
        Assert.True(thronglets2.Select(x=>x.Id).SequenceEqual(thronglets3.Select(x=>x.Id)));
        Assert.False(thronglets2.Select(x=>x.Personality).SequenceEqual(thronglets3.Select(x=>x.Personality)));
    }
    
    [Fact]
    public void PartialUpdate_WithConnectionWithEntity_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        connection.Insert<Gnome, string>(gnome);
        int updatedRecords = connection.PartialUpdate(
            gnome with { Name = "The Mule", Height = 140, IsActive = false},
            x=>x
                .CopyProperty(y=>y.Name)
                .CopyProperty(y=>y.Height)
        );
        Gnome? gnomeFromDb = connection.Get<Gnome, string>(gnome.Id);
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public void PartialUpdate_WithTransactionWithEntity_ShouldHaveChangedValue()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        transaction.Insert<Gnome, string>(gnome);
        int updatedRecords =transaction.PartialUpdate(
            gnome with { Name = "The Mule", Height = 140, IsActive = false},
            x=>x
                .CopyProperty(y=>y.Name)
                .CopyProperty(y=>y.Height)
        );
        Gnome? gnomeFromDb = transaction.Get<Gnome, string>(gnome.Id);
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public void PartialUpdate_WithConnectionWithIntKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Havord1",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = connection.Insert(dwarf);
        int updatedRecords = connection.PartialUpdate<Dwarf>(
            key: dwarfId,
            x => x
                    .SetProperty(y=>y.Name, "The Mule")
                    .SetProperty(y=>y.Height, 139)
        );
        Dwarf? dwarfFromDb = connection.Get<Dwarf>(dwarfId);
        
        //Assert
        Assert.True(dwarfFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(dwarfFromDb is { Name: "The Mule", Height: 139 });
    }
    
    [Fact]
    public void PartialUpdate_WithTransactionWithIntKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Dwarf dwarf = new()
        {
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = transaction.Insert(dwarf);
        int updatedRecords = transaction.PartialUpdate<Dwarf>(
            key: dwarfId,
            x => x
                .SetProperty(y=>y.Name, "Soupeter")
                .SetProperty(y=>y.Height, 110)
        );
        Dwarf? dwarfFromDb = transaction.Get<Dwarf>(dwarfId);
        
        //Assert
        Assert.True(dwarfFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(dwarfFromDb is { Name: "Soupeter", Height: 110 });
    }
    
    [Fact]
    public void PartialUpdate_WithConnectionWithTypedKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        connection.Insert<Gnome, string>(gnome);
        int updatedRecords = connection.PartialUpdate<Gnome, string>(
            gnome.Id,
            x=>x.SetProperty(y=>y.Name, "The Mule")
                .SetProperty(y=>y.Height, 140)
        );
        Gnome? gnomeFromDb = connection.Get<Gnome, string>(gnome.Id);
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public void PartialUpdate_WithTransactionWithTypedKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        transaction.Insert<Gnome, string>(gnome);
        int updatedRecords = transaction.PartialUpdate<Gnome, string>(
            gnome.Id,
            x=>x.SetProperty(y=>y.Name, "The Mule")
                .SetProperty(y=>y.Height, 140)
        );
        Gnome? gnomeFromDb = transaction.Get<Gnome, string>(gnome.Id);
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public void BulkPartialUpdate_WithConnectionWithEntities_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        connection.BulkInsert(gnomes);
        
        gnomes = gnomes.Select(x=> x with { Name = "The Mule", Height = 140, IsActive = false}).ToArray();
        
        //Act
        int updatedRecords = connection.BulkPartialUpdate(
            gnomes,
            x=>x
                .CopyProperty(y=>y.IsActive)
                .CopyProperty(y=>y.Height)
        );
        IEnumerable<Gnome> gnomesFromDb = connection.Get<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x.Name != "The Mule"));
    }
    
    [Fact]
    public void BulkPartialUpdate_WithTransactionWithEntities_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        transaction.BulkInsert(gnomes);
        
        gnomes = gnomes.Select(x=> x with { Name = "The Mule", Height = 140, IsActive = false}).ToArray();
        
        //Act
        int updatedRecords = transaction.BulkPartialUpdate(
            gnomes,
            x=>x
                .CopyProperty(y=>y.IsActive)
                .CopyProperty(y=>y.Height)
        );
        IEnumerable<Gnome> gnomesFromDb = transaction.Get<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x.Name != "The Mule"));
    }
    
    [Fact]
    public void BulkPartialUpdate_WithConnectionWithIntKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string sharedName = Guid.NewGuid().ToString("N");
        string whereFilter = $"{fixture.DbProvider.Options.NameConverter("Name")}='{sharedName}'";

        Dwarf[] dwarves = [new()
        {
            Name = sharedName,
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Name = sharedName,
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        connection.BulkInsert(dwarves);
        
        dwarves = connection.Get<Dwarf>(whereFilter).ToArray();
        
        //Act
        int updatedRecords = connection.BulkPartialUpdate<Dwarf>(
            keys: dwarves.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, false)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Dwarf> dwarvesFromDb = connection.Get<Dwarf>(whereFilter);
        
        //Assert
        Assert.True(dwarvesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(dwarvesFromDb.All(x => x.Name == sharedName && !x.IsActive));
    }
    
    [Fact]
    public void BulkPartialUpdate_WithTransactionWithIntKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        string sharedName = Guid.NewGuid().ToString("N");
        string whereFilter = $"{fixture.DbProvider.Options.NameConverter("Name")}='{sharedName}'";

        Dwarf[] dwarves = [new()
        {
            Name = sharedName,
            Height = 120,
            IsActive = false,
            Strength = 50
        }, new()
        {
            Name = sharedName,
            Height = 110,
            IsActive = false,
            Strength = 40
        }];
        
        transaction.BulkInsert(dwarves);
        
        dwarves = transaction.Get<Dwarf>(whereFilter).ToArray();
        
        //Act
        int updatedRecords = transaction.BulkPartialUpdate<Dwarf>(
            keys: dwarves.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, true)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Dwarf> dwarvesFromDb = transaction.Get<Dwarf>(whereFilter);
        
        transaction.Commit();
        
        //Assert
        Assert.True(dwarvesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(dwarvesFromDb.All(x => x.Name == sharedName && x.IsActive));
    }
    
    [Fact]
    public void BulkPartialUpdate_WithConnectionWithTypedKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        connection.BulkInsert(gnomes);
        
        //Act
        int updatedRecords = connection.BulkPartialUpdate<Gnome, string>(
            gnomes.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, false)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Gnome> gnomesFromDb = connection.Get<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x is { IsActive: false, Height: 140 }));
    }
    
    [Fact]
    public void BulkPartialUpdate_WithTransactionWithTypedKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        transaction.BulkInsert(gnomes);
        
        //Act
        int updatedRecords = transaction.BulkPartialUpdate<Gnome, string>(
            gnomes.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, false)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Gnome> gnomesFromDb = transaction.Get<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x is { IsActive: false, Height: 140 }));
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithConnectionWithEntity_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        await connection.InsertAsync<Gnome, string>(gnome);
        int updatedRecords = await connection.PartialUpdateAsync(
            gnome with { Name = "The Mule", Height = 140, IsActive = false},
            x=>x
                .CopyProperty(y=>y.Name)
                .CopyProperty(y=>y.Height)
        );
        Gnome? gnomeFromDb = await connection.GetAsync<Gnome, string>(gnome.Id);
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithTransactionWithEntity_ShouldHaveChangedValue()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        await transaction.InsertAsync<Gnome, string>(gnome);
        int updatedRecords = await transaction.PartialUpdateAsync(
            gnome with { Name = "The Mule", Height = 140, IsActive = false},
            x=>x
                .CopyProperty(y=>y.Name)
                .CopyProperty(y=>y.Height)
        );
        Gnome? gnomeFromDb = await transaction.GetAsync<Gnome, string>(gnome.Id);
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithConnectionWithIntKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Dwarf dwarf = new()
        {
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = await connection.InsertAsync(dwarf);
        int updatedRecords = await connection.PartialUpdateAsync<Dwarf>(
            key: dwarfId,
            x => x
                    .SetProperty(y=>y.Name, "The Mule")
                    .SetProperty(y=>y.Height, 140)
        );
        Dwarf? dwarfFromDb = await connection.GetAsync<Dwarf>(dwarfId);
        
        //Assert
        Assert.True(dwarfFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(dwarfFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithTransactionWithIntKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Dwarf dwarf = new()
        {
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        int dwarfId = await transaction.InsertAsync(dwarf);
        int updatedRecords = await transaction.PartialUpdateAsync<Dwarf>(
            key: dwarfId,
            x => x
                .SetProperty(y=>y.Name, "Soupeter")
                .SetProperty(y=>y.Height, 110)
        );
        Dwarf? dwarfFromDb = await transaction.GetAsync<Dwarf>(dwarfId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(dwarfFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(dwarfFromDb is { Name: "Soupeter", Height: 110 });
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithConnectionWithTypedKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        await connection.InsertAsync<Gnome, string>(gnome);
        int updatedRecords = await connection.PartialUpdateAsync<Gnome, string>(
            gnome.Id,
            x=>x.SetProperty(y=>y.Name, "The Mule")
                .SetProperty(y=>y.Height, 140)
        );
        Gnome? gnomeFromDb = await connection.GetAsync<Gnome, string>(gnome.Id);
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithTransactionWithTypedKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome gnome = new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        };
        
        //Act
        await transaction.InsertAsync<Gnome, string>(gnome);
        int updatedRecords = await transaction.PartialUpdateAsync<Gnome, string>(
            gnome.Id,
            x=>x.SetProperty(y=>y.Name, "The Mule")
                .SetProperty(y=>y.Height, 140)
        );
        Gnome? gnomeFromDb = await transaction.GetAsync<Gnome, string>(gnome.Id);
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomeFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(gnomeFromDb is { Name: "The Mule", Height: 140 });
    }
    
    [Fact]
    public async Task BulkPartialUpdateAsync_WithConnectionWithEntities_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        await connection.BulkInsertAsync(gnomes);
        
        gnomes = gnomes.Select(x=> x with { Name = "The Mule", Height = 140, IsActive = false}).ToArray();
        
        //Act
        int updatedRecords = await connection.BulkPartialUpdateAsync(
            gnomes,
            x=>x
                .CopyProperty(y=>y.IsActive)
                .CopyProperty(y=>y.Height)
        );
        IEnumerable<Gnome> gnomesFromDb = await connection.GetAsync<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x.Name != "The Mule"));
    }
    
    [Fact]
    public async Task BulkPartialUpdateAsync_WithTransactionWithEntities_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        await transaction.BulkInsertAsync(gnomes);
        
        gnomes = gnomes.Select(x=> x with { Name = "The Mule", Height = 140, IsActive = false}).ToArray();
        
        //Act
        int updatedRecords = await transaction.BulkPartialUpdateAsync(
            gnomes,
            x=>x
                .CopyProperty(y=>y.IsActive)
                .CopyProperty(y=>y.Height)
        );
        IEnumerable<Gnome> gnomesFromDb = await transaction.GetAsync<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x.Name != "The Mule"));
    }
    
    [Fact]
    public async Task BulkPartialUpdateAsync_WithConnectionWithIntKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string sharedName = Guid.NewGuid().ToString("N");
        string whereFilter = $"{fixture.DbProvider.Options.NameConverter("Name")}='{sharedName}'";

        Dwarf[] dwarves = [new()
        {
            Name = sharedName,
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Name = sharedName,
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        await connection.BulkInsertAsync(dwarves);
        
        dwarves = (await connection.GetAsync<Dwarf>(whereFilter)).ToArray();
        
        //Act
        int updatedRecords = await connection.BulkPartialUpdateAsync<Dwarf>(
            keys: dwarves.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, false)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Dwarf> dwarvesFromDb = await connection.GetAsync<Dwarf>(whereFilter);
        
        //Assert
        Assert.True(dwarvesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(dwarvesFromDb.All(x => x.Name == sharedName && !x.IsActive));
    }
    
    [Fact]
    public async Task BulkPartialUpdateAsync_WithTransactionWithIntKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        
        string sharedName = Guid.NewGuid().ToString("N");
        string whereFilter = $"{fixture.DbProvider.Options.NameConverter("Name")}='{sharedName}'";

        Dwarf[] dwarves = [new()
        {
            Name = sharedName,
            Height = 120,
            IsActive = false,
            Strength = 50
        }, new()
        {
            Name = sharedName,
            Height = 110,
            IsActive = false,
            Strength = 40
        }];
        
        await transaction.BulkInsertAsync(dwarves);
        
        dwarves = (await transaction.GetAsync<Dwarf>(whereFilter)).ToArray();
        
        //Act
        int updatedRecords = await transaction.BulkPartialUpdateAsync<Dwarf>(
            keys: dwarves.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, true)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Dwarf> dwarvesFromDb = await transaction.GetAsync<Dwarf>(whereFilter);
        
        transaction.Commit();
        
        //Assert
        Assert.True(dwarvesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(dwarvesFromDb.All(x => x.Name == sharedName && x.IsActive));
    }
    
    [Fact]
    public async Task BulkPartialUpdateAsync_WithConnectionWithTypedKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        await connection.BulkInsertAsync(gnomes);
        
        //Act
        int updatedRecords = await connection.BulkPartialUpdateAsync<Gnome, string>(
            gnomes.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, false)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Gnome> gnomesFromDb = await connection.GetAsync<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x is { IsActive: false, Height: 140 }));
    }
    
    [Fact]
    public async Task BulkPartialUpdateAsync_WithTransactionWithTypedKeys_ShouldHaveChangedValues()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Gnome[] gnomes = [new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Havord",
            Height = 120,
            IsActive = true,
            Strength = 50
        }, new()
        {
            Id = Guid.NewGuid().ToString("N"),
            Name = "Torord",
            Height = 110,
            IsActive = true,
            Strength = 40
        }];
        
        await transaction.BulkInsertAsync(gnomes);
        
        //Act
        int updatedRecords = await transaction.BulkPartialUpdateAsync<Gnome, string>(
            gnomes.Select(x=>x.Id).ToArray(),
            x=>x
                .SetProperty(y=>y.IsActive, false)
                .SetProperty(y=>y.Height, 140)
        );
        IEnumerable<Gnome> gnomesFromDb = await transaction.GetAsync<Gnome, string>(gnomes.Select(x=>x.Id).ToArray());
        
        transaction.Commit();
        
        //Assert
        Assert.True(gnomesFromDb.Count() == 2);
        Assert.True(updatedRecords == 2);
        Assert.True(gnomesFromDb.All(x => x is { IsActive: false, Height: 140 }));
    }
    
    [Fact]
    public void PartialUpdate_WithConnectionWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet = new()
        {
            Name = "Havord",
            Personality = Personality.Aloof
        };
        
        //Act
        ThrongletKey throngletKey = connection.Insert<Thronglet, ThrongletKey>(thronglet);
        int updatedRecords = connection.PartialUpdate<Thronglet, ThrongletKey>(
            key: throngletKey,
            x => x
                .SetProperty(y=>y.Personality, Personality.Assertive)
        );
        Thronglet? throngletFromDb = connection.Get<Thronglet, ThrongletKey>(throngletKey);
        
        //Assert
        Assert.True(throngletFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(throngletFromDb is { Personality: Personality.Assertive, Name: "Havord" });
    }
    
    [Fact]
    public void PartialUpdate_WithTransactionWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet = new()
        {
            Name = "Butterichard",
            Personality = Personality.Aloof
        };
        
        //Act
        ThrongletKey throngletKey = transaction.Insert<Thronglet, ThrongletKey>(thronglet);
        int updatedRecords = transaction.PartialUpdate<Thronglet, ThrongletKey>(
            key: throngletKey,
            x => x
                .SetProperty(y=>y.Personality, Personality.Assertive)
        );
        Thronglet? throngletFromDb = transaction.Get<Thronglet, ThrongletKey>(throngletKey);
        
        //Assert
        Assert.True(throngletFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(throngletFromDb is { Personality: Personality.Assertive, Name: "Butterichard" });
    }
    
    [Fact]
    public void PartialUpdate_WithConnectionWithEntityWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet = new()
        {
            Id = 1,
            Name = "Havord",
            Personality = Personality.Aloof
        };
        
        //Act
        
        connection.Insert<Thronglet, ThrongletKey>(thronglet);
        
        thronglet.Personality = Personality.Assertive;
        
        int updatedRecords = connection.PartialUpdate(
            thronglet,
            x => x.CopyProperty(y=>y.Personality)
        );
        Thronglet? throngletFromDb = connection.Get<Thronglet, ThrongletKey>(new (thronglet.Id, thronglet.Name));
        
        //Assert
        Assert.True(throngletFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(throngletFromDb is { Personality: Personality.Assertive, Name: "Havord" });
    }
    
    [Fact]
    public void PartialUpdate_WithTransactionWithEntityWithCompositeKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet = new()
        {
            Id = 2,
            Name = "Gavord",
            Personality = Personality.Aloof
        };
        
        //Act
        
        transaction.Insert<Thronglet, ThrongletKey>(thronglet);
        
        thronglet.Personality = Personality.Assertive;
        
        int updatedRecords = transaction.PartialUpdate(
            thronglet,
            x => x.CopyProperty(y=>y.Personality)
        );
        Thronglet? throngletFromDb = transaction.Get<Thronglet, ThrongletKey>(new (thronglet.Id, thronglet.Name));
        
        //Assert
        Assert.True(throngletFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(throngletFromDb is { Personality: Personality.Assertive, Name: "Gavord" });
    }

    [Fact]
    public void BulkUpdate_WithConnectionWithCustomWhere_ShouldHaveChangedValues()
    {
        //Arrange
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        string commonName = Guid.NewGuid().ToString("N");
        
        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 8,
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        connection.BulkInsert(gnomes);
        
        //Act
        Gnome payload = new()
        {
            Name = "Potatotto",
            Height = 999
        };
        
        int updatedRecords = connection.BulkUpdate(payload, $"{wrapper}{_nameColumn}{wrapper}='{commonName}'");
        
        //Assert
        Assert.True(updatedRecords == 3);
    }

    [Fact]
    public void BulkUpdate_WithTransactionWithCustomWhere_ShouldHaveChangedValues()
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
        
        string commonName = Guid.NewGuid().ToString("N");
        
        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 8,
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        transaction.BulkInsert(gnomes);
        
        //Act
        Gnome payload = new()
        {
            Name = "Potatotto",
            Height = 999
        };
        
        int updatedRecords = transaction.BulkUpdate(payload, $"{wrapper}{_nameColumn}{wrapper}='{commonName}'");
        
        //Assert
        Assert.True(updatedRecords == 3);
    }

    [Fact]
    public void BulkPartialUpdate_WithConnectionWithCustomWhere_ShouldHaveChangedValues()
    {
        //Arrange
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        
        string commonName = Guid.NewGuid().ToString("N");
        
        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 8,
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        connection.BulkInsert(gnomes);
        
        //Act
        Gnome payload = new()
        {
            Name = "Potatotto",
            Height = 999
        };
        
        int updatedRecords = connection.BulkPartialUpdate(
            payload,
            x=>x.CopyProperty(y=>y.Height),
            $"{wrapper}{_nameColumn}{wrapper}='{commonName}'");
        
        IEnumerable<Gnome> updatedGnomes = connection.Get<Gnome, string>(gnomes.Select(z=>z.Id).ToArray());
        
        //Assert
        Assert.True(updatedRecords == 3);
        Assert.True(updatedGnomes.All(x=>x.Name != "Potatotto" && x.Height == 999));
    }

    [Fact]
    public void BulkPartialUpdate_WithTransactionWithCustomWhere_ShouldHaveChangedValues()
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
        
        string commonName = Guid.NewGuid().ToString("N");
        
        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 8,
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        transaction.BulkInsert(gnomes);
        
        //Act
        Gnome payload = new()
        {
            Name = "Potatotto",
            Height = 999
        };
        
        int updatedRecords = transaction.BulkPartialUpdate(
            payload,
            x=>x.CopyProperty(y=>y.Height),
            $"{wrapper}{_nameColumn}{wrapper}='{commonName}'");
        
        IEnumerable<Gnome> updatedGnomes = transaction.Get<Gnome, string>(gnomes.Select(z=>z.Id).ToArray());
        
        //Assert
        Assert.True(updatedRecords == 3);
        Assert.True(updatedGnomes.All(x=>x.Name != "Potatotto" && x.Height == 999));
    }

    [Fact]
    public void BulkPartialUpdate_WithConnectionWithTypedKeyWithCustomWhere_ShouldHaveChangedValues()
    {
        //Arrange
        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        
        string commonName = Guid.NewGuid().ToString("N");
        
        Gnome[] gnomes = [
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 8,
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                IsActive = true,
                Strength = 1
            },
            new()
            {
                Id = Guid.NewGuid().ToString("N"),
                Name = commonName,
                Height = 120,
                IsActive = true,
                Strength = 999
            }
        ];
        
        connection.BulkInsert(gnomes);
        
        //Act
        
        int updatedRecords = connection.BulkPartialUpdate<Gnome, string>(
            gnomes.Select(x=>x.Id).ToArray(),
            x=>x.SetProperty(y=>y.Height, 7999),
            $"{wrapper}{_nameColumn}{wrapper}='{commonName}'");
        
        IEnumerable<Gnome> updatedGnomes = connection.Get<Gnome, string>(gnomes.Select(z=>z.Id).ToArray());
        
        //Assert
        Assert.True(updatedRecords == 3);
        Assert.True(updatedGnomes.All(x=>x.Height == 7999));
    }
    [Fact]
    public async Task UpdateAsync_WithConnectionWithAutoincrementingKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        DateTime newDateOfBirth = new DateTime(2001, 1, 1);
        
        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(2000, 1, 1)
        };
        int pixieId = await connection.InsertAsync(pixie);
        
        //Act
        pixie.Id = pixieId;
        pixie.DateOfBirth = newDateOfBirth;
        
        int updatedRecords = await connection.UpdateAsync(pixie);
        Pixie? pixieFromDb = await connection.GetAsync<Pixie>(pixieId);
        
        //Assert
        Assert.True(pixieFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(pixieFromDb.DateOfBirth == newDateOfBirth);
    }
    
    [Fact]
    public async Task PartialUpdateAsync_WithConnectionWithEntityWithAutoincrementingKey_ShouldHaveChangedValue()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        DateTime newDateOfBirth = new DateTime(2001, 1, 1);
        
        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(2000, 1, 1)
        };
        int pixieId = await connection.InsertAsync(pixie);
        
        //Act
        pixie.Id = pixieId;
        pixie.Ignored = "nope";
        pixie.DateOfBirth = newDateOfBirth;
        
        int updatedRecords = await connection.PartialUpdateAsync(pixie, x=>x.CopyProperty(y=>y.DateOfBirth));
        Pixie? pixieFromDb = await connection.GetAsync<Pixie>(pixieId);
        
        //Assert
        Assert.True(pixieFromDb is not null);
        Assert.True(updatedRecords == 1);
        Assert.True(pixieFromDb.DateOfBirth == newDateOfBirth && pixieFromDb.Ignored != "nope");
    }
}