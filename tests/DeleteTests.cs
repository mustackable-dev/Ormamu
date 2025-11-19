using System.Data;
using Dapper;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class DeleteTests(DbFixture fixture)
{
    private readonly string _idColumn = fixture.DbProvider.Options.NameConverter("Id");
    private readonly string _magicPower = fixture.DbProvider.Options.NameConverter("MagicPower");
    private readonly string _dateOfBirth = fixture.DbProvider.Options.NameConverter("DateOfBirth");
    
    [Fact]
    public void Delete_WithConnectionWithEntity_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = connection.Insert(pixie);
        
        //Act
        connection.Delete(pixie with { Id = pixieId });
        Pixie? pixie2 = connection.Get<Pixie>(pixieId);
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public void Delete_WithTransactionWithEntity_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = transaction.Insert(pixie);
        
        //Act
        transaction.Delete(pixie with { Id = pixieId });
        Pixie? pixie2 = transaction.Get<Pixie>(pixieId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public async Task DeleteAsync_WithConnectionWithEntity_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = await connection.InsertAsync(pixie);
        
        //Act
        await connection.DeleteAsync(pixie with { Id = pixieId });
        Pixie? pixie2 = await connection.GetAsync<Pixie>(pixieId);
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public async Task DeleteAsync_WithTransactionWithEntity_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = await transaction.InsertAsync(pixie);
        
        //Act
        await transaction.DeleteAsync(pixie with { Id = pixieId });
        Pixie? pixie2 = await transaction.GetAsync<Pixie>(pixieId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public void Delete_WithConnectionWithIntKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = connection.Insert(pixie);
        
        //Act
        connection.Delete<Pixie>(pixieId);
        Pixie? pixie2 = connection.Get<Pixie>(pixieId);
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public void Delete_WithTransactionWithIntKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = transaction.Insert(pixie);
        
        //Act
        transaction.Delete<Pixie>(pixieId);
        Pixie? pixie2 = transaction.Get<Pixie>(pixieId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public async Task DeleteAsync_WithConnectionWithIntKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = await connection.InsertAsync(pixie);
        
        //Act
        await connection.DeleteAsync<Pixie>(pixieId);
        Pixie? pixie2 = await connection.GetAsync<Pixie>(pixieId);
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public async Task DeleteAsync_WithTransactionWithIntKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        int pixieId = await transaction.InsertAsync(pixie);
        
        //Act
        await transaction.DeleteAsync<Pixie>(pixieId);
        Pixie? pixie2 = await transaction.GetAsync<Pixie>(pixieId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public void Delete_WithConnectionWithTypedKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        long pixieId = connection.Insert<Pixie, long>(pixie);
        
        //Act
        connection.Delete<Pixie, long>(pixieId);
        Pixie? pixie2 = connection.Get<Pixie, long>(pixieId);
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public void Delete_WithTransactionWithTypedKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        long pixieId = transaction.Insert<Pixie, long>(pixie);
        
        //Act
        transaction.Delete<Pixie, long>(pixieId);
        Pixie? pixie2 = transaction.Get<Pixie, long>(pixieId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public async Task DeleteAsync_WithConnectionWithTypedKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        long pixieId = await connection.InsertAsync<Pixie, long>(pixie);
        
        //Act
        await connection.DeleteAsync<Pixie, long>(pixieId);
        Pixie? pixie2 = await connection.GetAsync<Pixie, long>(pixieId);
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public async Task DeleteAsync_WithTransactionWithTypedKey_ShouldNotFindEntry()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie pixie = new()
        {
            MagicPower = 10,
            DateOfBirth = new DateTime(1985, 1, 1),
        };
        long pixieId = await transaction.InsertAsync<Pixie, long>(pixie);
        
        //Act
        await transaction.DeleteAsync<Pixie, long>(pixieId);
        Pixie? pixie2 = await transaction.GetAsync<Pixie, long>(pixieId);
        
        transaction.Commit();
        
        //Assert
        Assert.True(pixie2 is null);
    }
    
    [Fact]
    public void BulkDelete_WithConnectionWithEntity_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = connection.Insert(pixie);
        }
        
        
        //Act
        int deletedRecords = connection.BulkDelete(pixies);
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(connection.Get<Pixie>((int)pixie.Id) is null);   
        }
    }
    
    [Fact]
    public void BulkDelete_WithTransactionWithEntity_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = transaction.Insert(pixie);
        }
        
        
        //Act
        int deletedRecords = transaction.BulkDelete(pixies);
        transaction.Commit();
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(connection.Get<Pixie>((int)pixie.Id) is null);
        }
    }
    
    
    [Fact]
    public void BulkDelete_WithConnectionWithIntKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = connection.Insert(pixie);
        }
        
        
        //Act
        int deletedRecords = connection.BulkDelete<Pixie>(pixies.Select(x=>(int)x.Id).ToArray());
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(connection.Get<Pixie>((int)pixie.Id) is null);   
        }
    }
    
    [Fact]
    public void BulkDelete_WithTransactionWithIntKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = transaction.Insert(pixie);
        }
        
        
        //Act
        int deletedRecords = transaction.BulkDelete<Pixie>(pixies.Select(x=>(int)x.Id).ToArray());
        transaction.Commit();
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(connection.Get<Pixie>((int)pixie.Id) is null);
        }
    }
    
    [Fact]
    public void BulkDelete_WithConnectionWithTypedKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = connection.Insert<Pixie, long>(pixie);
        }
        
        
        //Act
        int deletedRecords = connection.BulkDelete<Pixie, long>(pixies.Select(x=>x.Id).ToArray());
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(connection.Get<Pixie, long>(pixie.Id) is null);
        }
    }
    
    [Fact]
    public void BulkDelete_WithTransactionWithTypedKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = transaction.Insert<Pixie, long>(pixie);
        }
        
        
        //Act
        int deletedRecords = transaction.BulkDelete<Pixie, long>(pixies.Select(x=>x.Id).ToArray());
        transaction.Commit();
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(connection.Get<Pixie, long>(pixie.Id) is null);
        }
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithConnectionWithEntity_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = await connection.InsertAsync(pixie);
        }
        
        
        //Act
        int deletedRecords = await connection.BulkDeleteAsync(pixies);
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(await connection.GetAsync<Pixie>((int)pixie.Id) is null);
        }
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithTransactionWithEntity_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = await transaction.InsertAsync(pixie);
        }
        
        
        //Act
        int deletedRecords = await transaction.BulkDeleteAsync(pixies);
        transaction.Commit();
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(await connection.GetAsync<Pixie>((int)pixie.Id) is null);
        }
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithConnectionWithIntKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = await connection.InsertAsync(pixie);
        }
        
        
        //Act
        int deletedRecords = await connection.BulkDeleteAsync<Pixie>(pixies.Select(x=>(int)x.Id).ToArray());
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(await connection.GetAsync<Pixie>((int)pixie.Id) is null);
        }
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithTransactionWithIntKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = await transaction.InsertAsync(pixie);
        }
        
        
        //Act
        int deletedRecords = await transaction.BulkDeleteAsync<Pixie>(pixies.Select(x=>(int)x.Id).ToArray());
        transaction.Commit();
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(await connection.GetAsync<Pixie>((int)pixie.Id) is null);
        }
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithConnectionWithTypedKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = await connection.InsertAsync<Pixie, long>(pixie);
        }
        
        
        //Act
        int deletedRecords = await connection.BulkDeleteAsync<Pixie, long>(pixies.Select(x=>x.Id).ToArray());
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(await connection.GetAsync<Pixie, long>(pixie.Id) is null);
        }
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithTransactionWithTypedKey_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            }
        ];
        
        foreach (Pixie pixie in pixies)
        {
            pixie.Id = await transaction.InsertAsync<Pixie, long>(pixie);
        }
        
        
        //Act
        int deletedRecords = await transaction.BulkDeleteAsync<Pixie, long>(pixies.Select(x=>x.Id).ToArray());
        transaction.Commit();
        
        //Assert
        Assert.True(deletedRecords == 2);
        foreach (Pixie pixie in pixies)
        {
            Assert.True(await connection.GetAsync<Pixie, long>(pixie.Id) is null);
        }
    }
    
    [Fact]
    public void Delete_WithConnectionOnMulticonfig_ShouldNotFindEntry()
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
        int deletedImp = connection.Delete<Imp, Guid>(impId);
        Imp? imp3 = connection.Get<Imp, Guid>(impId);
        //Assert
        Assert.True(imp2 is not null);
        Assert.True(deletedImp == 1);
        Assert.True(imp3 is null);
    }
    
    [Fact]
    public void BulkDelete_WithConnectionOnMulticonfig_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.MulticonfigDbProvider.GetConnection();
        
        int impsSampleSize = 110;
        string name = "Hamburger";
        
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
        int deleteImps = connection.BulkDelete<Imp, Guid>(imps.Select(x=>x.GuidKey).ToArray());
        IEnumerable<Imp> imps3 = connection.Get<Imp>($"\"{TestsConfig.CustomColumnName}\"=@templateName", commandParams: new { templateName = name });
        //Assert
        
        Assert.True(insertedImps == impsSampleSize);
        Assert.True(imps2 is not null);
        Assert.True(imps2.Count() == impsSampleSize);
        Assert.True(deleteImps == impsSampleSize);
        Assert.True(!imps3.Any());
    }
    
    [Fact]
    public void Delete_WithConnectionWithCompositeKey_ShouldNotFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 13,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = connection.Insert<Thronglet, ThrongletKey>(thronglet);
        //Act
        ThrongletKey key = new(Id: 13, Name: "Lemonadesther");
        int deletedCount = connection.Delete<Thronglet, ThrongletKey>(key);
        Thronglet? thronglet2 = connection.Get<Thronglet, ThrongletKey>(key);
        //Assert
        Assert.True(insertedKey == key);
        Assert.True(deletedCount == 1);
        Assert.Null(thronglet2);
    }
    
    [Fact]
    public void Delete_WithTransactionWithCompositeKey_ShouldNotFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet =
            new()
            {
                Id = 14,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = transaction.Insert<Thronglet, ThrongletKey>(thronglet);
        //Act
        ThrongletKey key = new(Id: 14, Name: "Lemonadesther");
        int deletedCount = transaction.Delete<Thronglet, ThrongletKey>(key);
        Thronglet? thronglet2 = transaction.Get<Thronglet, ThrongletKey>(key);
        transaction.Commit();
        //Assert
        Assert.True(insertedKey == key);
        Assert.True(deletedCount == 1);
        Assert.Null(thronglet2);
    }
    
    [Fact]
    public async Task DeleteAsync_WithConnectionWithCompositeKey_ShouldNotFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        Thronglet thronglet =
            new()
            {
                Id = 15,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = await connection.InsertAsync<Thronglet, ThrongletKey>(thronglet);
        //Act
        ThrongletKey key = new(Id: 15, Name: "Lemonadesther");
        int deletedCount = await connection.DeleteAsync<Thronglet, ThrongletKey>(key);
        Thronglet? thronglet2 = await connection.GetAsync<Thronglet, ThrongletKey>(key);
        //Assert
        Assert.True(insertedKey == key);
        Assert.True(deletedCount == 1);
        Assert.Null(thronglet2);
    }
    
    [Fact]
    public async Task DeleteAsync_WithTransactionWithCompositeKey_ShouldNotFindEntry()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        Thronglet thronglet =
            new()
            {
                Id = 16,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            };
        
        ThrongletKey insertedKey = await transaction.InsertAsync<Thronglet, ThrongletKey>(thronglet);
        //Act
        ThrongletKey key = new(Id: 16, Name: "Lemonadesther");
        int deletedCount = await transaction.DeleteAsync<Thronglet, ThrongletKey>(key);
        Thronglet? thronglet2 = await transaction.GetAsync<Thronglet, ThrongletKey>(key);
        transaction.Commit();
        //Assert
        Assert.True(insertedKey == key);
        Assert.True(deletedCount == 1);
        Assert.Null(thronglet2);
    }
    
    [Fact]
    public void BulkDelete_WithConnectionWithCompositeKey_ShouldNotFindEntries()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 21,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 22,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 23,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            }
        ];
        
        int insertedThronglets = connection.BulkInsert(thronglets);
        //Act
        
        int deletedThronglets = connection.BulkDelete<Thronglet, ThrongletKey>(
            thronglets.Skip(1).Select(x=>new ThrongletKey(x.Id, x.Name)).ToArray());
        IEnumerable<Thronglet> thronglets2 = connection.Get<Thronglet>(
            $"{wrapper}{_idColumn}{wrapper}>=@id AND {wrapper}{_idColumn}{wrapper}<=@id2",
            commandParams: new { id = 21, id2 = 23 });
        //Assert
        Assert.True(insertedThronglets == 3);
        Assert.True(deletedThronglets == 2);
        Assert.True(thronglets2.Count()==1);
    }
    
    [Fact]
    public void BulkDelete_WithTransactionWithCompositeKey_ShouldNotFindEntries()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 24,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 25,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 26,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            }
        ];
        
        int insertedThronglets = transaction.BulkInsert(thronglets);
        
        //Act
        int deletedThronglets = transaction.BulkDelete<Thronglet, ThrongletKey>(
            thronglets.Reverse().Skip(1).Select(x=>new ThrongletKey(x.Id, x.Name)).ToArray());
        IEnumerable<Thronglet> thronglets2 = transaction.Get<Thronglet>(
            $"{wrapper}{_idColumn}{wrapper}>=@id AND {wrapper}{_idColumn}{wrapper}<=@id2",
            commandParams: new { id = 24, id2 = 26 });
        transaction.Commit();
        
        //Assert
        Assert.True(insertedThronglets == 3);
        Assert.True(deletedThronglets == 2);
        Assert.True(thronglets2.Count()==1);
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithConnectionWithCompositeKey_ShouldNotFindEntries()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 27,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 28,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 29,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            }
        ];
        
        int insertedThronglets = await connection.BulkInsertAsync(thronglets);
        //Act
        
        int deletedThronglets = await connection.BulkDeleteAsync<Thronglet, ThrongletKey>(
            thronglets.Skip(2).Select(x=>new ThrongletKey(x.Id, x.Name)).ToArray());
        IEnumerable<Thronglet> thronglets2 = await connection.GetAsync<Thronglet>(
            $"{wrapper}{_idColumn}{wrapper}>=@id AND {wrapper}{_idColumn}{wrapper}<=@id2",
            commandParams: new { id = 27, id2 = 29 });
        //Assert
        Assert.True(insertedThronglets == 3);
        Assert.True(deletedThronglets == 1);
        Assert.True(thronglets2.Count()==2);
    }
    
    [Fact]
    public async Task BulkDeleteAsync_WithTransactionWithCompositeKey_ShouldNotFindEntries()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };

        Thronglet[] thronglets =
        [
            new()
            {
                Id = 30,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 31,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            },
            new()
            {
                Id = 32,
                Name = "Lemonadesther",
                Personality = Personality.Assertive
            }
        ];
        
        int insertedThronglets = await transaction.BulkInsertAsync(thronglets);
        
        //Act
        int deletedThronglets = await transaction.BulkDeleteAsync<Thronglet, ThrongletKey>(
            thronglets.Reverse().Skip(2).Select(x=>new ThrongletKey(x.Id, x.Name)).ToArray());
        
        DynamicParameters queryParameters = new();
        queryParameters.AddDynamicParams(new { id = 30, id2 = 32 });
        
        IEnumerable<Thronglet> thronglets2 = await transaction.GetAsync<Thronglet>(
            $"{wrapper}{_idColumn}{wrapper}>=@id AND {wrapper}{_idColumn}{wrapper}<=@id2",
            commandParams: queryParameters);
        transaction.Commit();
        
        //Assert
        Assert.True(insertedThronglets == 3);
        Assert.True(deletedThronglets == 1);
        Assert.True(thronglets2.Count()==2);
    }
    
    [Fact]
    public void BulkDelete_WithConnectionWithCustomWhere_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();

        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };

        Pixie[] pixies = [
            new()
            {
                MagicPower = 10,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(1990, 1, 1),
            },
            new()
            {
                MagicPower = 30,
                DateOfBirth = new DateTime(2000, 1, 1),
                Ignored = "yep",
            }
        ];
        
        connection.BulkInsert(pixies);
        
        //Act
        int deletedRecords = connection.BulkDelete<Pixie>(
            $"{wrapper}{_dateOfBirth}{wrapper}<@dateOfBirth",
            new { dateOfBirth = new DateTime(2000, 1, 1) });
        
        //Assert
        Assert.True(deletedRecords == 2);
    }
    
    [Fact]
    public void BulkDelete_WithTransactionWithCustomWhere_ShouldNotFindEntries()
    {
        //Arrange
        
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();

        string wrapper = fixture.DbProvider.Options.Dialect switch
        {
            SqlDialect.PostgreSql => "\"",
            SqlDialect.MySql or SqlDialect.MariaDb => "`",
            _ => ""
        };
        
        Pixie[] pixies = [
            new()
            {
                MagicPower = 990,
                DateOfBirth = new DateTime(1985, 1, 1),
            },
            new()
            {
                MagicPower = 1000,
                DateOfBirth = new DateTime(1990, 1, 1),
            },
            new()
            {
                MagicPower = 1200,
                DateOfBirth = new DateTime(2000, 1, 1),
                Ignored = "yep",
            }
        ];
        
        transaction.BulkInsert(pixies);
        
        //Act
        int deletedRecords = transaction.BulkDelete<Pixie>(
            $"{wrapper}{_magicPower}{wrapper}>@magicPower",
            new { magicPower = 980 });
        
        //Assert
        Assert.True(deletedRecords == 3);
    }
}