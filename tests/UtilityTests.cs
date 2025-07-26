using System.Data;
using Ormamu;
using OrmamuTests.Entities;
using OrmamuTests.Fixtures;

namespace OrmamuTests;

public class UtilityTests(DbFixture fixture)
{
    private readonly string _ageColumn = fixture.DbProvider.Options.NameConverter("Age");
    private readonly string _isActiveColumn = fixture.DbProvider.Options.NameConverter("IsActive");
    private readonly string _magicPowerColumn = fixture.DbProvider.Options.NameConverter("MagicPower");
    private readonly string _dateOfBirthColumn = fixture.DbProvider.Options.NameConverter("DateOfBirth");
    
    [Fact]
    public void Count_WithConnectionTotal_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int goblin = connection.Count<Goblin, int>();
        //Assert
        Assert.True(goblin == 30);
    }
    
    [Fact]
    public void Count_WithTransactionTotal_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int goblin = transaction.Count<Goblin, int>();
        transaction.Commit();
        //Assert
        Assert.True(goblin == 30);
    }
    
    [Fact]
    public async Task CountAsync_WithConnectionTotal_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int goblin = await connection.CountAsync<Goblin, int>();
        //Assert
        Assert.True(goblin == 30);
    }
    
    [Fact]
    public async Task CountAsync_WithTransactionTotal_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int goblin = await transaction.CountAsync<Goblin, int>();
        transaction.Commit();
        //Assert
        Assert.True(goblin == 30);
    }
    
    [Fact]
    public void Count_WithConnectionWithWhere_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int goblin = connection.Count<Goblin, int>($"{_ageColumn}>@age", new { age = 120 });
        //Assert
        Assert.True(goblin == 3);
    }
    
    [Fact]
    public void Count_WithTransactionWithWhere_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int goblin = transaction.Count<Goblin, int>($"{_ageColumn}>@age", new { age = 120 });
        transaction.Commit();
        //Assert
        Assert.True(goblin == 3);
    }
    
    [Fact]
    public async Task CountAsync_WithConnectionWithWhere_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int goblin = await connection.CountAsync<Goblin, int>($"{_ageColumn}>@age", new { age = 120 });
        //Assert
        Assert.True(goblin == 3);
    }
    
    [Fact]
    public async Task CountAsync_WithTransactionWithWhere_ShouldReturnCorrectCount()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int goblin = await transaction.CountAsync<Goblin, int>($"{_ageColumn}>@age", new { age = 120 });
        transaction.Commit();
        //Assert
        Assert.True(goblin == 3);
    }
    
    [Fact]
    public void Sum_WithConnectionTotal_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int sumAge = connection.Sum<Goblin, int>(x=>x.Age);
        //Assert
        Assert.True(sumAge == 2101);
    }
    
    [Fact]
    public void Sum_WithTransactionTotal_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int sumAge = transaction.Sum<Goblin, int>(x=>x.Age);
        transaction.Commit();
        //Assert
        Assert.True(sumAge == 2101);
    }
    
    [Fact]
    public async Task SumAsync_WithConnectionTotal_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int sumAge = await connection.SumAsync<Goblin, int>(x=>x.Age);
        //Assert
        Assert.True(sumAge == 2101);
    }
    
    [Fact]
    public async Task SumAsync_WithTransactionTotal_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int sumAge = await transaction.SumAsync<Goblin, int>(x=>x.Age);
        transaction.Commit();
        //Assert
        Assert.True(sumAge == 2101);
    }
    
    [Fact]
    public void Sum_WithConnectionWithWhere_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int sumStamina = connection.Sum<Goblin, int>(
            x=>x.Stamina,
            $"{_isActiveColumn}=@IsActive",
            new { IsActive = true });
        //Assert
        Assert.True(sumStamina == 645);
    }
    
    [Fact]
    public void Sum_WithTransactionWithWhere_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int sumStamina = transaction.Sum<Goblin, int>(
            x=>x.Stamina,
            $"{_isActiveColumn}=@IsActive",
            new { IsActive = true });
        transaction.Commit();
        //Assert
        Assert.True(sumStamina == 645);
    }
    
    [Fact]
    public async Task SumAsync_WithConnectionWithWhere_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        int sumStamina = await connection.SumAsync<Goblin, int>(
            x=>x.Stamina,
            $"{_isActiveColumn}=@IsActive",
            new { IsActive = true });
        //Assert
        Assert.True(sumStamina == 645);
    }
    
    [Fact]
    public async Task SumAsync_WithTransactionWithWhere_ShouldReturnCorrectSum()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        int sumStamina = await transaction.SumAsync<Goblin, int>(
            x=>x.Stamina,
            $"{_isActiveColumn}=@IsActive",
            new { IsActive = true });
        transaction.Commit();
        //Assert
        Assert.True(sumStamina == 645);
    }
    
    [Fact]
    public void Average_WithConnectionTotal_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        double averageAgility = connection.Average<Goblin, double>(x=>x.Agility);
        //Assert
        Assert.True(Math.Abs(averageAgility - 8.413333333333334) < 0.01);
    }
    
    [Fact]
    public void Average_WithTransactionTotal_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        double averageAgility = transaction.Average<Goblin, double>(x=>x.Agility);
        transaction.Commit();
        //Assert
        Assert.True(Math.Abs(averageAgility - 8.413333333333334) < 0.01);
    }
    
    [Fact]
    public async Task AverageAsync_WithConnectionTotal_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        double averageAgility = await connection.AverageAsync<Goblin, double>(x=>x.Agility);
        //Assert
        Assert.True(Math.Abs(averageAgility - 8.413333333333334) < 0.01);
    }
    
    [Fact]
    public async Task AverageAsync_WithTransactionTotal_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        double averageAgility = await transaction.AverageAsync<Goblin, double>(x=>x.Agility);
        transaction.Commit();
        //Assert
        Assert.True(Math.Abs(averageAgility - 8.413333333333334) < 0.01);
    }
    
    [Fact]
    public void Average_WithConnectionWithWhere_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        double averageAgility = connection.Average<Goblin, double>(
            x=>x.Agility,
            $"{_dateOfBirthColumn}>@Date",
            new { Date = new DateTime(2022, 1, 1) });
        //Assert
        Assert.True(averageAgility == 9.1);
    }
    
    [Fact]
    public void Average_WithTransactionWithWhere_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        double averageAgility = transaction.Average<Goblin, double>(
            x=>x.Agility,
            $"{_dateOfBirthColumn}>@Date",
            new { Date = new DateTime(2022, 1, 1) });
        transaction.Commit();
        //Assert
        Assert.True(averageAgility == 9.1);
    }
    
    [Fact]
    public async Task AverageAsync_WithConnectionWithWhere_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        double averageAgility = await connection.AverageAsync<Goblin, double>(
            x=>x.Agility,
            $"{_dateOfBirthColumn}>@Date",
            new { Date = new DateTime(2022, 1, 1) });
        //Assert
        Assert.True(averageAgility == 9.1);
    }
    
    [Fact]
    public async Task AverageAsync_WithTransactionWithWhere_ShouldReturnCorrectAverage()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        double averageAgility = await transaction.AverageAsync<Goblin, double>(
            x=>x.Agility,
            $"{_dateOfBirthColumn}>@Date",
            new { Date = new DateTime(2022, 1, 1) });
        transaction.Commit();
        //Assert
        Assert.True(averageAgility == 9.1);
    }
    
    [Fact]
    public void Min_WithConnectionTotal_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal minSalary = connection.Min<Goblin, decimal>(x=>x.Salary);
        //Assert
        Assert.True(minSalary == (decimal)12000.5);
    }
    
    [Fact]
    public void Min_WithTransactionTotal_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal minSalary = transaction.Min<Goblin, decimal>(x=>x.Salary);
        transaction.Commit();
        //Assert
        Assert.True(minSalary == (decimal)12000.5);
    }
    
    [Fact]
    public async Task MinAsync_WithConnectionTotal_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal minSalary = await connection.MinAsync<Goblin, decimal>(x=>x.Salary);
        //Assert
        Assert.True(minSalary == (decimal)12000.5);
    }
    
    [Fact]
    public async Task MinAsync_WithTransactionTotal_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal minSalary = await transaction.MinAsync<Goblin, decimal>(x=>x.Salary);
        transaction.Commit();
        //Assert
        Assert.True(minSalary == (decimal)12000.5);
    }
    
    [Fact]
    public void Min_WithConnectionWithWhere_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal minSalary = connection.Min<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower =  8000000000 });
        //Assert
        Assert.True(minSalary == (decimal)21000.2);
    }
    
    [Fact]
    public void Min_WithTransactionWithWhere_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal minSalary = transaction.Min<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower =  8000000000 });
        transaction.Commit();
        //Assert
        Assert.True(minSalary == (decimal)21000.2);
    }
    
    [Fact]
    public async Task MinAsync_WithConnectionWithWhere_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal minSalary = await connection.MinAsync<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower =  8000000000 });
        //Assert
        Assert.True(minSalary == (decimal)21000.2);
    }
    
    [Fact]
    public async Task MinAsync_WithTransactionWithWhere_ShouldReturnCorrectMin()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal minSalary = await transaction.MinAsync<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower =  8000000000 });
        transaction.Commit();
        //Assert
        Assert.True(minSalary == (decimal)21000.2);
    }
    
    [Fact]
    public void Max_WithConnectionTotal_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal maxSalary = connection.Max<Goblin, decimal>(x=>x.Salary);
        //Assert
        Assert.True(maxSalary == 62000);
    }
    
    [Fact]
    public void Max_WithTransactionTotal_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal maxSalary = transaction.Max<Goblin, decimal>(x=>x.Salary);
        transaction.Commit();
        //Assert
        Assert.True(maxSalary == 62000);
    }
    
    [Fact]
    public async Task MaxAsync_WithConnectionTotal_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal maxSalary = await connection.MaxAsync<Goblin, decimal>(x=>x.Salary);
        //Assert
        Assert.True(maxSalary == 62000);
    }
    
    [Fact]
    public async Task MaxAsync_WithTransactionTotal_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal maxSalary = await transaction.MaxAsync<Goblin, decimal>(x=>x.Salary);
        transaction.Commit();
        //Assert
        Assert.True(maxSalary == 62000);
    }
    
    [Fact]
    public void Max_WithConnectionWithWhere_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal maxSalary = connection.Max<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower = 8000000000 });
        //Assert
        Assert.True(maxSalary == 59000);
    }
    
    [Fact]
    public void Max_WithTransactionWithWhere_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal maxSalary = transaction.Max<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower = 8000000000 });
        transaction.Commit();
        //Assert
        Console.WriteLine(maxSalary);
        Assert.True(maxSalary == 59000);
    }
    
    [Fact]
    public async Task MaxAsync_WithConnectionWithWhere_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        //Act
        decimal maxSalary = await connection.MaxAsync<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower = 8000000000 });
        //Assert
        Assert.True(maxSalary == 59000);
    }
    
    [Fact]
    public async Task MaxAsync_WithTransactionWithWhere_ShouldReturnCorrectMax()
    {
        //Arrange
        using IDbConnection connection = fixture.DbProvider.GetConnection();
        connection.Open();
        using IDbTransaction transaction = connection.BeginTransaction();
        //Act
        decimal maxSalary = await transaction.MaxAsync<Goblin, decimal>(
            x=>x.Salary,
            $"{_magicPowerColumn}>@MagicPower",
            new { MagicPower = 8000000000 });
        transaction.Commit();
        //Assert
        Assert.True(maxSalary == 59000);
    }
}