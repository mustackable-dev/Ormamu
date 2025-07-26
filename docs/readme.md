[![Mustackable](https://avatars.githubusercontent.com/u/200509271?s=96&v=4)](https://mustackable.dev)

<!-- TOC -->
  * [Intro](#intro)
  * [Features](#features)
  * [Installation](#installation)
  * [Quick Start](#quick-start)
  * [Supported Operations](#supported-operations)
    * [Read Operations](#read-operations)
    * [Create Operations](#create-operations)
    * [Update Operations](#update-operations)
    * [Delete Operations](#delete-operations)
    * [Utility Operations](#utility-operations)
      * [Notes](#notes)
  * [Configuration](#configuration)
    * [Basic Configuration](#basic-configuration)
    * [Custom Configuration](#custom-configuration)
    * [Multiple Database Configurations](#multiple-database-configurations)
  * [Built-in Name Converters](#built-in-name-converters)
  * [Primary Keys](#primary-keys)
    * [Supported Types](#supported-types)
    * [Autoincrementing](#autoincrementing)
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
- Support for Multiple SQL dialects and naming conventions in the same project
- Flexible primary key handling (custom names, types, and composite keys)
- Minimal setup via standard `DataAnnotations`
- High-performance query generation via structure data caching (no runtime reflection)
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

Then we need to call Ormamu.Configuration.Apply() at the start of our project to initialize the query composition engine. Like this:

```csharp

var builder = WebApplication.CreateBuilder(args);

builder.Host.AddLogging(builder.Configuration);

builder.Services
    .AddSecurity(builder.Configuration)
    .AddSwagger()
    .AddTransientServices()
    .AddHostedServices()
    .AddWorker()
    .AddControllers();

// Uses default settings (SQL Server dialect, direct name mapping)
Ormamu.Configuration.Apply(); 

var app = builder.Build();
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

gnomeFromDb!.Name = "Mopey";

//Here we update entity with the new name
connection.Update(gnomeFromDb);

//In order to delete the entity, you just need to 
//pass the value of its primary key
connection.Delete<Gnome>(gnomeId);

```

You can find more examples in [the tests suite](https://github.com/mustackable-dev/Ormamu/tree/main/tests).

## Supported Operations

### Read Operations

| Operation                          | Method (Sync / Async)                          | Contexts                          | Description                                                                 |
|------------------------------------|------------------------------------------------|-----------------------------------|-----------------------------------------------------------------------------|
| Get by `int` key                   | `Get<T>(int)` / `GetAsync<T>(int)`            | `IDbConnection`, `IDbTransaction` | Retrieves an entity by integer key.                                         |
| Get by custom or composite key     | `Get<TKey, T>(TKey)` / `GetAsync<TKey, T>(TKey)` | `IDbConnection`, `IDbTransaction` | Retrieves an entity by any key type (supports composite keys).              |
| Bulk fetch with filters/pagination | `Get<T>(...)` / `GetAsync<T>(...)`            | `IDbConnection`, `IDbTransaction` | Retrieves a list of entities with optional `WHERE`, `ORDER BY`, pagination. |

---

### Create Operations

| Operation                         | Method (Sync / Async)                               | Contexts                          | Description                                                                  |
|-----------------------------------|------------------------------------------------------|-----------------------------------|------------------------------------------------------------------------------|
| Insert by `int` key               | `Insert<T>(T entity)` / `InsertAsync<T>(T entity)`  | `IDbConnection`, `IDbTransaction` | Inserts an entity with `int` key and returns the generated key.             |
| Insert by custom or composite key | `Insert<TKey, T>(T entity)` / `InsertAsync<TKey, T>(T entity)` | `IDbConnection`, `IDbTransaction` | Inserts an entity and returns a custom key (supports composite).    |
| Bulk insert (array)               | `BulkInsert<T>(T[] entities)` / `BulkInsertAsync<T>(T[] entities)` | `IDbConnection`, `IDbTransaction` | Inserts multiple entities in batches (default batch size: 100).             |

---

### Update Operations

| Operation                    | Method (Sync / Async)                                                   | Contexts                          | Description                                                                                     |
|------------------------------|------------------------------------------------------------------------|-----------------------------------|-------------------------------------------------------------------------------------------------|
| Update single entity          | `Update<T>(T entity)` / `UpdateAsync<T>(T entity)`                     | `IDbConnection`, `IDbTransaction` | Updates a single entity and returns the number of affected rows (typically 1).                  |
| Bulk update (array)           | `BulkUpdate<T>(T[] entities, int batchSize = 100)` / `BulkUpdateAsync<T>(T[] entities, int batchSize = 100)` | `IDbConnection`, `IDbTransaction` | Updates multiple entities in batches (default batch size: 100) and returns total affected rows.|

---

### Delete Operations

| Operation                         | Method (Sync / Async)                                               | Contexts                          | Description                                                                                    |
|-----------------------------------|--------------------------------------------------------------------|-----------------------------------|------------------------------------------------------------------------------------------------|
| Delete by `int` key               | `Delete<TValue>(int key)` / `DeleteAsync<TValue>(int key)`         | `IDbConnection`, `IDbTransaction` | Deletes a single entity by `int` key, returns number of affected rows (typically 1).           |
| Delete by custom or composite key | `Delete<TKey, TValue>(TKey key)` / `DeleteAsync<TKey, TValue>(TKey key)` | `IDbConnection`, `IDbTransaction` | Deletes a single entity by a custom key (also supports composite keys), returns affected rows. |
| Bulk delete by `int` keys         | `BulkDelete<TValue>(int[] keys, int batchSize = 100)` / `BulkDeleteAsync<TValue>(int[] keys, int batchSize = 100)` | `IDbConnection`, `IDbTransaction` | Deletes multiple entities by array of `int` keys in batches, returns total deleted count.      |
| Bulk delete by custom keys        | `BulkDelete<TKey, TValue>(TKey[] keys, int batchSize = 100)` / `BulkDeleteAsync<TKey, TValue>(TKey[] keys, int batchSize = 100)` | `IDbConnection`, `IDbTransaction` | Deletes multiple entities by array of custom keys in batches, returns total deleted count.     |

---

### Utility Operations

| Operation                    | Method (Sync / Async)                                                   | Contexts                          | Description                                                                                     |
|------------------------------|------------------------------------------------------------------------|-----------------------------------|-------------------------------------------------------------------------------------------------|
| Count entities               | `Count<TEntity, TValue>(...)` / `CountAsync<TEntity, TValue>(...)`     | `IDbConnection`, `IDbTransaction` | Counts the number of entities optionally filtered by a WHERE clause, returns numeric count.     |
| Sum property values          | `Sum<TEntity, TValue>(...)` / `SumAsync<TEntity, TValue>(...)`         | `IDbConnection`, `IDbTransaction` | Calculates the sum of a specified property optionally filtered by a WHERE clause.               |
| Average property values      | `Average<TEntity, TValue>(...)` / `AverageAsync<TEntity, TValue>(...)` | `IDbConnection`, `IDbTransaction` | Calculates the average of a specified property optionally filtered by a WHERE clause.           |
| Minimum property values      | `Min<TEntity, TValue>(...)` / `MinAsync<TEntity, TValue>(...)`         | `IDbConnection`, `IDbTransaction` | Gets the minimum value of a specified property optionally filtered by a WHERE clause.           |
| Maximum property values      | `Max<TEntity, TValue>(...)` / `MaxAsync<TEntity, TValue>(...)`         | `IDbConnection`, `IDbTransaction` | Gets the maximum value of a specified property optionally filtered by a WHERE clause.           |

#### Notes

Each of the Utility operations (except for `Count`) requires an Expression parameter. This parameter specifies the target entity property on which the operation will be performed.

---

## Configuration

Ormamu offers flexible configuration options to suit different project needs, including support for multiple databases with varying SQL dialects and naming conventions.

### Basic Configuration

You can configure Ormamu globally using default settings. By default, it uses the **SQL Server** dialect and assumes no custom mapping between entity property names and database column names.

```csharp
Ormamu.Configuration.Apply();
```

This applies the configuration to all entities in your application.

### Custom Configuration

To customize the behavior, create an instance of `OrmamuOptions` where you can specify:

- **Dialect** — The SQL dialect for your database (e.g., SQL Server, MySQL, PostgreSQL).
- **NameConverter** — A function to convert entity property names to database column names (e.g., snake_case, kebab-case).
- **PropertyBindingFlags** — Reflection flags for selecting entity properties (default is `Public | Instance`).
- **ConfigId** — An optional identifier to differentiate configurations when working with multiple databases.

**Example:**

```csharp
OrmamuOptions options = new ()
{
    Dialect = SqlDialect.PostgreSql,
    NameConverter = NameConverters.ToSnakeCase
};

Ormamu.Configuration.Apply(options);
```

### Multiple Database Configurations

If your project connects to multiple databases with different naming conventions or dialects, you can configure multiple `OrmamuOptions` instances and assign each entity a configuration ID using the `[ConfigId]` attribute.

```csharp
OrmamuOptions[] configs = new[]
{
    new OrmamuOptions
    {
        ConfigId = "MainDb",
        Dialect = SqlDialect.SqlServer,
        NameConverter = NameConverters.ToUpperCase
    },
    new OrmamuOptions
    {
        ConfigId = "AnalyticsDb",
        Dialect = SqlDialect.PostgreSql,
        NameConverter = NameConverters.ToSnakeCase
    }
};

Ormamu.Configuration.Apply(configs);
```

Then mark your entity classes with the corresponding configuration ID:

```csharp
[ConfigId("MainDb")]
[Table("Users", Schema = "app")]
public class User
{
    [Key]
    public int Id { get; set; }
    public string UserName { get; set; }
}

[ConfigId("AnalyticsDb")]
[Table("Events", Schema = "diagnostics")]
public class EventLog
{
    [Key]
    public int Id { get; set; }
    public string EventName { get; set; }
}
```

## Built-in Name Converters

Ormamu provides common name converters for convenience:

- `NameConverters.ToSnakeCase` — Converts `PropertyName` to `property_name`
- `NameConverters.ToKebabCase` — Converts `PropertyName` to `property-name`
- `NameConverters.ToUpperCase` — Converts `PropertyName` to `PROPERTYNAME`
- `NameConverters.ToLowerCase` — Converts `PropertyName` to `propertyname`

You can also bring your own custom converter by providing a ```Func<string, string>``` delegate in your OrmamuOptions.

## Primary Keys

### Supported Types

You can use any ValueType or string as a primary key for your entity model, as long as it maps properly to your database.

Since the most common practice is to use an autoincrementing int or long primary key, Ormamu comes in with shorthand methods specifically for this case.

However, you are free to choose whatever key type suits your needs.

All `IDbConnection` and `IDbTransaction` extension methods in Ormamu offer overloads that allow you to specify your primary key type.

### Autoincrementing

By default, Ormamu assumes that a property marked with the `[Key]` attribute is autoincrementing (and therefore omits this property from insert statements).

If your key is not autoincrementing, use the `[DatabaseGenerated(DatabaseGeneratedOption.None)]` attribute like this:

```csharp
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.None)]
    public Guid GuidKey { get; set; }
```

Then you will need to supply a value for the key during Create/Insert operations.

### Composite Keys

Ormamu supports composite keys for entities. For each entity that uses a composite key, you must define a dedicated type (class, record, or struct) that represents the structure of the composite key.

This type should have properties whose names map one-to-one with the entity property names that comprise the composite key.

Finally, decorate your entity model with the `[CompositeKey]` attribute to indicate that it uses a composite key.

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
    public int Id { get; set; }
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
ThrongletKey insertedKey = await transaction.InsertAsync<ThrongletKey, Thronglet>(thronglet);

//Here we can generate a new instance of the ThrongletKey
//to do some querying.
ThrongletKey key = new(Id: 17, Name: "Lemonadesther");
Thronglet? thronglet2 = await transaction.GetAsync<ThrongletKey, Thronglet>(key);

//Or we can just use the 'insertedKey' value from our insert operation

Thronglet? thronglet3 = await transaction.GetAsync<ThrongletKey, Thronglet>(insertedKey);

transaction.Commit();
```

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



