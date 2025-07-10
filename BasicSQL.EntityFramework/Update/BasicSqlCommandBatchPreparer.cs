using Microsoft.EntityFrameworkCore.Update;
using Microsoft.EntityFrameworkCore.Update.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;
using System.Linq;
using System;

namespace BasicSQL.EntityFramework.Update
{
    public class BasicSqlCommandBatchPreparer : CommandBatchPreparer
    {
        public BasicSqlCommandBatchPreparer(CommandBatchPreparerDependencies dependencies)
            : base(dependencies)
        {
            Console.WriteLine("BasicSqlCommandBatchPreparer: Constructor");
        }

        public override IEnumerable<ModificationCommandBatch> BatchCommands(
            IList<IUpdateEntry> entries,
            IUpdateAdapter updateAdapter)
        {
            Console.WriteLine($"--- BatchCommands START ---");
            Console.WriteLine($"Batching {entries.Count} entries.");

            foreach (var entry in entries)
            {
                if (entry.EntityState == Microsoft.EntityFrameworkCore.EntityState.Added)
                {
                    Console.WriteLine($"Processing Added entry for {entry.EntityType.ShortName()}");
                    foreach (var prop in entry.EntityType.GetProperties())
                    {
                        if (prop.IsPrimaryKey())
                        {
                            var currentValue = entry.GetCurrentValue(prop);
                            var isTemporary = entry.IsTemporary(prop);
                            Console.WriteLine($"  PK {prop.Name}: Value='{currentValue}', IsTemporary={isTemporary}");
                        }
                    }
                }
            }

            try
            {
                var batches = base.BatchCommands(entries, updateAdapter);
                var batchList = batches.ToList();
                Console.WriteLine($"Successfully created {batchList.Count} batches.");
                Console.WriteLine($"--- BatchCommands END ---");
                return batchList;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"!!! EXCEPTION in base.BatchCommands: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine($"--- BatchCommands FAILED ---");
                throw; // Re-throw the exception to allow the test to fail as expected
            }
        }
    }
}