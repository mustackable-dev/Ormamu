using System.Data;
using Dapper;
using Dapper.Transaction;

namespace Ormamu;

internal static class BulkExecution
{
    internal static int ExecuteBulk<TValue>(
        this IDbConnection connection,
        Func<TValue[], int, int, CommandComponents> commandFactory,
        TValue[] values,
        int batchSize = 100,
        bool limitProcessedEntries = false)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += limitProcessedEntries ? 
                Math.Min(connection.Execute(components.Command, components.Parameters), currentBatchSize) :
                connection.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
    
    internal static int ExecuteBulk<TValue>(
        this IDbTransaction transaction,
        Func<TValue[], int, int, CommandComponents> commandFactory,
        TValue[] values,
        int batchSize = 100,
        bool limitProcessedEntries = false)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += limitProcessedEntries ? 
                Math.Min(transaction.Execute(components.Command, components.Parameters), currentBatchSize) :
                transaction.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
    
    internal static async Task<int> ExecuteBulkAsync<TValue>(
        this IDbConnection connection,
        Func<TValue[], int, int, CommandComponents> commandFactory,
        TValue[] values,
        int batchSize = 100,
        bool limitProcessedEntries = false)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += limitProcessedEntries ? 
                Math.Min(await connection.ExecuteAsync(components.Command, components.Parameters), currentBatchSize) :
                await connection.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
    
    internal static async Task<int> ExecuteBulkAsync<TValues>(
        this IDbTransaction transaction,
        Func<TValues[], int, int, CommandComponents> commandFactory,
        TValues[] values,
        int batchSize = 100,
        bool limitProcessedEntries = false)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += limitProcessedEntries ? 
                Math.Min(await transaction.ExecuteAsync(components.Command, components.Parameters), currentBatchSize) :
                await transaction.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
}
internal record CommandComponents(string Command, DynamicParameters Parameters);