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
        int batchSize = 100)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += connection.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
    
    internal static int ExecuteBulk<TValue>(
        this IDbTransaction transaction,
        Func<TValue[], int, int, CommandComponents> commandFactory,
        TValue[] values,
        int batchSize = 100)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += transaction.Execute(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
    
    internal static async Task<int> ExecuteBulkAsync<TValue>(
        this IDbConnection connection,
        Func<TValue[], int, int, CommandComponents> commandFactory,
        TValue[] values,
        int batchSize = 100)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += await connection.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
    
    internal static async Task<int> ExecuteBulkAsync<TValues>(
        this IDbTransaction transaction,
        Func<TValues[], int, int, CommandComponents> commandFactory,
        TValues[] values,
        int batchSize = 100)
    {
        int processedEntries = 0;
        
        int remaining = values.Length;
        while (remaining>0)
        {
            int currentBatchSize = Math.Min(batchSize, remaining);
            CommandComponents components = commandFactory(values, processedEntries, processedEntries + currentBatchSize);
            
            processedEntries += await transaction.ExecuteAsync(components.Command, components.Parameters);
            
            remaining -= currentBatchSize;
        }
        
        return processedEntries;
    }
}
internal record CommandComponents(string Command, DynamicParameters Parameters);