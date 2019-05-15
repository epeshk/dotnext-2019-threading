using System;
using System.Threading;
using System.Threading.Tasks;

namespace SyncContinuation
{
    class Program
    {
        static void Main(string[] args)
        {
            var tcs = new TaskCompletionSource<bool>();
//            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

/* Task 1 */
            var task1 = Task.Run(() =>
            {
                Console.WriteLine(1);
                tcs.TrySetResult(true);
                Console.WriteLine(2);
            });
            
/* Task 2 */
            var task2 = Task.Run(async () =>
            {
                await tcs.Task;
                Thread.Sleep(-1);
            });

            Task.WhenAll(task1, task2).GetAwaiter().GetResult();
        }
    }
}