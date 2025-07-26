using System.Data;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class UpdateTests(DbFixture fixture)
{
    private readonly string _heightColumn = fixture.DbProvider.Options.NameConverter("Height");
    private readonly string _idColumn = fixture.DbProvider.Options.NameConverter("Id");
    
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
        string gnomeId = connection.Insert<string, Gnome>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Height = 130;
        int updatedRecords = connection.Update(gnome);
        Gnome? gnomeFromDb = connection.Get<string, Gnome>(gnomeId);
        
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
        string gnomeId = transaction.Insert<string, Gnome>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Height = 130;
        int updatedRecords = transaction.Update(gnome);
        Gnome? gnomeFromDb = transaction.Get<string, Gnome>(gnomeId);
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
        string gnomeId = await connection.InsertAsync<string, Gnome>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Name = "Mazelin";
        int updatedRecords = await connection.UpdateAsync(gnome);
        Gnome? gnomeFromDb = await connection.GetAsync<string, Gnome>(gnomeId);
        
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
        string gnomeId = await transaction.InsertAsync<string, Gnome>(gnome);
        
        //Act
        gnome.Id = gnomeId;
        gnome.Name = "Sni";
        int updatedRecords = await transaction.UpdateAsync(gnome);
        Gnome? gnomeFromDb = await transaction.GetAsync<string, Gnome>(gnomeId);
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
        int height = 999;
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
            gnome.Id=connection.Insert<string, Gnome>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = connection.BulkUpdate(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = connection.Get<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", param: new {height});
        
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
            gnome.Id=transaction.Insert<string, Gnome>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = transaction.BulkUpdate(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = transaction.Get<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", param: new {height});
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
            gnome.Id= await connection.InsertAsync<string, Gnome>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = await connection.BulkUpdateAsync(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = await connection.GetAsync<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", param: new {height});
        
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
            gnome.Id= await transaction.InsertAsync<string, Gnome>(gnome);
            if(gnome.Height == height)
                gnome.Name = title;
        }
        
        //Act
        int updatedRecords = await transaction.BulkUpdateAsync(gnomes.Where(x=>x.Height == height).ToArray());
        IEnumerable<Gnome> gnomesFromDb = await transaction.GetAsync<Gnome>($"{wrapper}{_heightColumn}{wrapper} = @height", param: new {height});
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
        
        Guid impId = connection.Insert<Guid, Imp>(imp);
        Imp? imp2 = connection.Get<Guid, Imp>(impId);
        imp.Name = "MegaPuratino";
        int updatedCount = connection.Update(imp);
        Imp? imp3 = connection.Get<Guid, Imp>(impId);
        
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
        IEnumerable<Imp> imps2 = connection.Get<Imp>($"\"{TestsConfig.CustomColumnName}\"=@templateName", param: new { templateName = name });
        imps = imps.Select(x =>
        {
            x.Name = "Milano";
            return x;
        }).ToArray();
        int updatedImps = connection.BulkUpdate(imps);
        IEnumerable<Imp> imps3 = connection.Get<Imp>($"\"{TestsConfig.CustomColumnName}\"=@templateName", param: new { templateName = "Milano" });
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
        
        ThrongletKey insertedKey = connection.Insert<ThrongletKey, Thronglet>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = connection.Update(thronglet);
        Thronglet? thronglet2 = connection.Get<ThrongletKey, Thronglet>(insertedKey);
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
        
        ThrongletKey insertedKey = transaction.Insert<ThrongletKey, Thronglet>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = transaction.Update(thronglet);
        Thronglet? thronglet2 = transaction.Get<ThrongletKey, Thronglet>(insertedKey);
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
        
        ThrongletKey insertedKey = await connection.InsertAsync<ThrongletKey, Thronglet>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = await connection.UpdateAsync(thronglet);
        Thronglet? thronglet2 = await connection.GetAsync<ThrongletKey, Thronglet>(insertedKey);
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
        
        ThrongletKey insertedKey = await transaction.InsertAsync<ThrongletKey, Thronglet>(thronglet);
        thronglet.Personality = Personality.Reflective;
        //Act
        int updatedCount = await transaction.UpdateAsync(thronglet);
        Thronglet? thronglet2 = await transaction.GetAsync<ThrongletKey, Thronglet>(insertedKey);
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
            param: new { id = 37, id2 = 39 });
        
        //Act
        
        int updatedThronglets = connection.BulkUpdate(thronglets.Select(x =>
        {
            x.Personality = Personality.Reflective;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = connection.Get<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            param: new { id = 37, id2 = 39 });
        
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
            param: new { id = 40, id2 = 42 });
        
        //Act
        
        int updatedThronglets = transaction.BulkUpdate(thronglets.Select(x =>
        {
            x.Personality = Personality.Aloof;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = transaction.Get<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            param: new { id = 40, id2 = 42 });
        
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
            param: new { id = 43, id2 = 45 });
        
        //Act
        
        int updatedThronglets = await connection.BulkUpdateAsync(thronglets.Select(x =>
        {
            x.Personality = Personality.Reflective;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = await connection.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            param: new { id = 43, id2 = 45 });
        
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
        IEnumerable<Thronglet> thronglets2 = await transaction.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            param: new { id = 46, id2 = 48 });
        
        //Act
        
        int updatedThronglets = await transaction.BulkUpdateAsync(thronglets.Select(x =>
        {
            x.Personality = Personality.Aloof;
            return x;
        }).ToArray());
        
        IEnumerable<Thronglet> thronglets3 = await transaction.GetAsync<Thronglet>(
            $"{_idColumn}>=@id AND {_idColumn}<=@id2",
            param: new { id = 46, id2 = 48 });

        transaction.Commit();
        
        //Assert
        
        Assert.True(updatedThronglets == 3);
        Assert.True(thronglets2.Count()==3);
        Assert.True(thronglets2.Select(x=>x.Id).SequenceEqual(thronglets3.Select(x=>x.Id)));
        Assert.False(thronglets2.Select(x=>x.Personality).SequenceEqual(thronglets3.Select(x=>x.Personality)));
    }
}