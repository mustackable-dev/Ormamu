[![Mustackable](https://avatars.githubusercontent.com/u/200509271?s=96&v=4)](https://mustackable.dev)

<!-- TOC -->
  * [Intro](#intro)
  * [Features](#features)
  * [Installation](#installation)
  * [Quick Start](#quick-start)
  * [Supported Operations](#supported-operations)
  * [Configuration](#configuration)
    * [Default](#default)
    * [Single Database Configuration](#single-database-configuration)
    * [Multiple Database Configurations](#multiple-database-configurations)
  * [Built-in Name Converters](#built-in-name-converters)
  * [Primary Keys](#primary-keys)
    * [Supported Types](#supported-types)
    * [Auto-Incrementing](#auto-incrementing)
    * [Composite Keys](#composite-keys)
  * [Excluding Properties](#excluding-properties)
  * [Specifying Custom Property Names](#specifying-custom-property-names)
  * [License](#license)
<!-- TOC -->

## Intro

Ormamu is a lightweight, mini ORM built on top of [Dapper](https://github.com/DapperLib/Dapper).
 
Ormamu hits the sweet spot between the convenience of Entity Framework and the minimal setup and performance of Dapper.

## Features

- Support for MSSQL, PostgreSQL, MySQL, MariaDB, and SQLite
- Support for multiple SQL dialects and naming conventions in the same project
- Flexible primary key support - supports standard `int` keys, typed keys and composite keys
- Minimal setup via standard `DataAnnotations` and stateless execution
- High-performance query generation via structure data caching
- Synchronous and asynchronous CRUD and bulk operations
- Utility operations (count, sum, average, min, max)
- Compatible with `IDbConnection` and `IDbTransaction`

## Installation

```bash
dotnet add package Ormamu
````

## Quick Start

Define your entity model like this:

```csharp
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

[Table("Gnomes", Schema = "MythicalCreatures")]
public class Gnome
{
  [Key]
  public int Id { get; set; }
  public string Name { get; set; } = null!;
  public float Strength { get; set; }
  public int Height { get; set; }
  public bool IsActive { get; set; }
  public bool? HobbitAncestry { get; set; }
}
```

Here we use the `[Table]` attribute to specify the schema and the name of the table in the database that corresponds to the Gnome entity. Then we tag the primary key property with the `[Key]` attribute.

Then we call `OrmamuConfig.Apply` to initialize the query composition engine with PostgreSQL support (you do not need to explicitly call this, if you use SQL Server and direct name mapping).

```csharp

using Ormamu;

var builder = WebApplication.CreateBuilder(args);

builder.Host.AddLogging(builder.Configuration);

builder.Services
    .AddSecurity(builder.Configuration)
    .AddSwagger()
    .AddTransientServices()
    .AddHostedServices()
    .AddWorker()
    .AddControllers();

// You only need to add these two lines, if you are using a custom 
// database setup (default is SQL Server with direct name mapping)

OrmamuOptions options = new() { Dialect = SqlDialect.PostgreSql };

OrmamuConfig.Apply(options);
```

Finally, we can do this:

```csharp

string _connectionString = "connection-to-db";
Gnome gnome = new Gnome { Name = "Bumpy", Strength = 100 };

using IDbConnection connection = new SqlConnection(_connectionString);

//By default, the insert command will return 
//the primary key value of the inserted entity
int gnomeId = connection.Insert(gnome);

//You can retrieve the newly-created entity via 
//its primary key's value
Gnome? gnomeFromDb = connection.Get<Gnome>(gnomeId);

//Here we update the entity with a new name
gnomeFromDb!.Name = "Mopey";
connection.Update(gnomeFromDb);

//.. and finally we delete the entity
connection.Delete(gnomeFromDb);

```

You can find more examples in [the tests suite](https://github.com/mustackable-dev/Ormamu/tree/main/tests).

## Supported Operations

All operations provided by Ormamu have synchronous and asynchronous versions that run either on an `IDbConnection` or an `IDbTransaction`.

Additionally, Ormamu fully supports typed and composite primary keys for entities, while also providing shorthand overrides for the most common case of using `int` primary keys.

- Single and bulk inserts are supported
- Single and bulk reads are supported, with optional filtering, ordering and pagination
- Single and bulk full updates are supported, as well as partial updates. Partial updates use a syntax similar to [ExecuteUpdate](https://learn.microsoft.com/en-us/ef/core/saving/execute-insert-update-delete), and can either set concrete properties' values from runtime or copy property values from an entity instance
- Single and bulk deletes are supported via key value, entity instance or custom `WHERE` clause
- Count, Sum, Average, Min and Max utility commands are supported
---

## Configuration

Ormamu provides a flexible configuration system designed to support multiple databases, SQL dialects, and naming conventions — allowing you to tailor its behavior to your application’s data model.

Ormamu can be configured via the `OrmamuConfig.Apply` method.

### Default

If `OrmamuConfig.Apply` is not explicitly called, Ormamu will lazily apply a default configuration (SQL Server dialect, no custom name mapping) the first time any database operation is executed. This default configuration is initialized only once.

Configuration can be safely reapplied at any time using `OrmamuConfig.Apply` — if a configuration already exists, it will be replaced with the new one.

This allows recovery from accidental default initialization or dynamic reconfiguration in testing environments.

### Single Database Configuration

You can pass an instance of `OrmamuOptions` to the `OrmamuConfig.Apply` to specify the following parameters:

- **Dialect** — The SQL dialect of your database (e.g., SQL Server, MySQL, PostgreSQL).
- **NameConverter** — A function to convert entity property names to database column names (e.g., snake_case, kebab-case).
- **PropertyBindingFlags** — Reflection flags for selecting entity properties (default is `Public | Instance`).

**Example:**

```csharp
OrmamuOptions options = new ()
{
    Dialect = SqlDialect.PostgreSql,
    NameConverter = NameConverters.ToSnakeCase
};

OrmamuConfig.Apply(options);
```

### Multiple Database Configurations

If your project connects to multiple databases with different naming conventions or dialects, you can configure multiple `OrmamuOptions` instances and assign each entity definition a configuration ID using the `[OrmamuConfigId]` attribute.

```csharp
Dictionary<object, OrmamuOptions> configs = new()
{
    {
        "MainDb", new OrmamuOptions {
            Dialect = SqlDialect.SqlServer,
            NameConverter = NameConverters.ToUpperCase
        }
    },
    {
        "AnalyticsDb", new OrmamuOptions
        {
            Dialect = SqlDialect.PostgreSql,
            NameConverter = NameConverters.ToSnakeCase
        }
    }
};

OrmamuConfig.Apply(configs);
```

Then mark your entity classes with the corresponding configuration ID attributes:

```csharp
[OrmamuConfigId("MainDb")]
[Table("Users", Schema = "app")]
public class User
{
    [Key]
    public int Id { get; set; }
    public string UserName { get; set; }
}

[OrmamuConfigId("AnalyticsDb")]
[Table("Events", Schema = "diagnostics")]
public class EventLog
{
    [Key]
    public int Id { get; set; }
    public string EventName { get; set; }
}
```
**IMPORTANT!**

Only `ValueType` (such as `int`, `bool`, `double`, `char`, `enum`, etc.) and `string` objects can be used as configuration IDs.

## Built-in Name Converters

Ormamu provides common name converters for convenience:

- `NameConverters.ToSnakeCase` — Converts `PropertyName` to `property_name`
- `NameConverters.ToKebabCase` — Converts `PropertyName` to `property-name`
- `NameConverters.ToUpperCase` — Converts `PropertyName` to `PROPERTYNAME`
- `NameConverters.ToLowerCase` — Converts `PropertyName` to `propertyname`

You can also bring your own custom converter by providing a ```Func<string, string>``` delegate in your `OrmamuOptions`.

## Primary Keys

### Supported Types

You can use any `ValueType` or `string` as a primary key for your entity model, as long as it maps properly to your database.

Since the most common practice is to use auto-incrementing `int` primary keys, Ormamu offers shorthand methods specifically for this case.

However, you are free to choose whatever key type suits your needs.

All `IDbConnection` and `IDbTransaction` extension methods in Ormamu offer overloads that allow you to specify your primary key type.

### Auto-Incrementing

By default, Ormamu assumes that a property marked with the `[Key]` attribute is auto-incrementing (and therefore omits this property from insert statements).

If your key is not auto-incrementing, use the `[DatabaseGenerated(DatabaseGeneratedOption.None)]` attribute like this:

```csharp
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public Guid GuidKey { get; set; }
```

Then you will need to supply a value for the key during Create/Insert operations.

### Composite Keys

Ormamu supports composite keys for entities. You just need to decorate all the components of your composite key with the `[Key]` attribute (just like you would normally do for traditional primary keys).

For each entity that uses a composite key, it is strongly recommended to define a dedicated type (class, record, or struct) that represents the structure of the composite key. _**Note: This is mandatory, if you plan to use `Get`, `BulkDelete`, `BulkUpdate` or `BulkPartialUpdate` operations with an array of composite key instances.**_

This type should have properties whose names map one-to-one with the entity property names that comprise the composite key.

Finally, decorate your model with the `[CompositeKey]` attribute to bind your composite key type to your entity.

For example:

```csharp

//This is the type for the composite key I will use for
//the thronglet table
public record struct ThrongletKey(int Id, string Name);

//I am specifying the type of the composite key to use
//for the thronglet entity with this custom CompositeKey
//attribute
[CompositeKey(typeof(ThrongletKey))]
[Table("Thronglets", Schema = "MythicalCreatures")]
public class Thronglet
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public int Id { get; set; }
    
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public string Name { get; set; } = null!;
    public Personality Personality { get; set; }
}
```

Then you can do some operations like this:

```csharp
using IDbTransaction transaction = connection.BeginTransaction();

Thronglet thronglet =
    new()
    {
        Id = 17,
        Name = "Lemonadesther",
        Personality = Personality.Assertive
    };

//After the insert operation completes, Ormamu returns
//an instance of the composite key type which you can reuse
ThrongletKey insertedKey = await transaction.InsertAsync<Thronglet, ThrongletKey>(thronglet);

//Here we can generate a new instance of the ThrongletKey
//to do some querying.
ThrongletKey key = new(Id: 17, Name: "Lemonadesther");
Thronglet? thronglet2 = await transaction.GetAsync<Thronglet, ThrongletKey>(key);

//Or we can just use the 'insertedKey' value from our insert operation

Thronglet? thronglet3 = await transaction.GetAsync<Thronglet, ThrongletKey>(insertedKey);

transaction.Commit();
```
Ormamu also supports composite keys, where one component is an auto-incrementing value. As mentioned in [Auto-incrementing](#auto-incrementing), if an entity property is marked with the `[Key]` property, Ormamu will assume it is auto-incrementing, unless you tag it with `[DatabaseGenerated(DatabaseGeneratedOption.None)]` property as well.

## Excluding Properties

You can tell Ormamu to exclude certain properties from your entity model by utilizing the `[NotMapped]` attribute like this:

```csharp
public class Pixie: EntityBase<long>
{
    public long MagicPower { get; set; }
    public DateTime DateOfBirth { get; set; }
    [NotMapped]
    public string Ignored { get; set; }
}
```

## Specifying Custom Property Names

You can also explicitly specify a column name for a property via the `[Column]` attribute:

```csharp
public class Imp
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public Guid GuidKey { get; set; }
    
    [Column("ime-na-imp")]
    public string Name { get; set; } = null!;
    public char FavouriteLetter { get; set; }
    public short Age { get; set; }
}
```

A property name specified via the `[Column]` attribute will take precedence over the value generated by the NameConverter you specified in your `OrmamuOptions`.

## License

MIT



