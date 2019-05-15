using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions.Extensions;
using NUnit.Framework;

namespace Dictionaries.Tests
{
    [TestFixture]
//    [Explicit]
    internal class StripedDictionary_Tests_Smoke_Concurrent
    {
        private Guid[] keys;
        private StripedDictionary<Guid, string, FastGuidComparer> dic;
        private HashSet<Guid> allKeys;
        private List<Task> tasks;
        private CancellationTokenSource cancellation;

        [SetUp]
        public void TestSetup()
        {
            keys = Enumerable.Range(0, 100).Select(_ => Guid.NewGuid()).ToArray();

            allKeys = new HashSet<Guid>(keys);

            dic = new StripedDictionary<Guid, string, FastGuidComparer>();

            for (int i = 0; i < keys.Length / 2; i++)
            {
                dic.Add(keys[i], keys[i].ToString());
            }

            tasks = new List<Task>();

            cancellation = new CancellationTokenSource();
        }

        [Test]
        public void Writer_contention_with_readers_should_not_cause_any_problems()
        {
            var testTime = 20.Seconds();

            LaunchWriter(testTime, cancellation.Token);

            LaunchReader(testTime, cancellation.Token);
            LaunchReader(testTime, cancellation.Token);
            LaunchReader(testTime, cancellation.Token);
            LaunchReader(testTime, cancellation.Token);

            Task.WhenAny(tasks).ContinueWith(_ => cancellation.Cancel());
            Task.WhenAll(tasks).GetAwaiter().GetResult();
        }

        [Test]
        public void Writer_contention_with_enumerators_should_not_cause_any_problems()
        {
            var testTime = 20.Seconds();

            LaunchWriter(testTime, cancellation.Token);

            LaunchEnumerator(testTime, cancellation.Token);
            LaunchEnumerator(testTime, cancellation.Token);
            LaunchEnumerator(testTime, cancellation.Token);
            LaunchEnumerator(testTime, cancellation.Token);

            Task.WhenAny(tasks).ContinueWith(_ => cancellation.Cancel());
            Task.WhenAll(tasks).GetAwaiter().GetResult();
        }

        [Test]
        public void Contention_between_enumerators_should_not_cause_any_problems()
        {
            var testTime = 20.Seconds();

            foreach (var key in keys)
            {
                dic.Set(key, key.ToString());
            }

            LaunchStrictEnumerator(testTime, cancellation.Token);
            LaunchStrictEnumerator(testTime, cancellation.Token);
            LaunchStrictEnumerator(testTime, cancellation.Token);
            LaunchStrictEnumerator(testTime, cancellation.Token);

            Task.WhenAny(tasks).ContinueWith(_ => cancellation.Cancel());
            Task.WhenAll(tasks).GetAwaiter().GetResult();
        }

        private void LaunchWriter(TimeSpan time, CancellationToken cancellationToken)
        {
            var task = Task.Run(() =>
            {
                var random = new Random(Guid.NewGuid().GetHashCode());
                
                var watch = Stopwatch.StartNew();

                try
                {
                    while (watch.Elapsed < time && !cancellationToken.IsCancellationRequested)
                    {
                        var index = random.Next(keys.Length / 2, keys.Length);

                        if (random.NextDouble() <= 0.5)
                        {
                            dic.Set(keys[index], keys[index].ToString());
                        }
                        else
                        {
                            dic.Remove(keys[index]);
                        }
                    }
                }
                catch (Exception error)
                {
                    throw new Exception("Error in writer!", error);
                }
            });

            tasks.Add(task);
        }

        private void LaunchReader(TimeSpan time, CancellationToken cancellationToken)
        {
            var watch = Stopwatch.StartNew();
            var lastRead = watch.ElapsedTicks;

            var readerTask = Task.Run(() =>
            {
                var random = new Random(Guid.NewGuid().GetHashCode());
                try
                {
                    while (watch.Elapsed < time && !cancellationToken.IsCancellationRequested)
                    {
                        var index = random.Next(keys.Length);

                        var found = dic.TryGetValue(keys[index], out var value);

                        if (!found && index < keys.Length / 2)
                            throw new Exception("Failed to find a key which never gets removed!");

                        if (found && value != keys[index].ToString())
                            throw new Exception("TryGetValue() returned some garbage as value!");

                        Interlocked.Exchange(ref lastRead, watch.ElapsedTicks);
                    }
                }
                catch (Exception error)
                {
                    Console.Out.WriteLine(error);

                    throw new Exception("Error in reader!", error);
                }
            });

            var checkerTask = Task.Run(() =>
            {
                while (watch.Elapsed < time && !cancellationToken.IsCancellationRequested)
                {
                    Thread.Sleep(50);

                    var diff = new TimeSpan(watch.ElapsedTicks - Interlocked.Read(ref lastRead));
                    if (diff.TotalSeconds >= 0.5)
                    {
                        throw new Exception("A reader hangs!");
                    }
                }
            });

            tasks.Add(readerTask);
            tasks.Add(checkerTask);
        }

        private void LaunchEnumerator(TimeSpan time, CancellationToken cancellationToken)
        {
            var task = Task.Run(() =>
            {
                var watch = Stopwatch.StartNew();

                var checkerSet = new HashSet<Guid>();

                try
                {
                    while (watch.Elapsed < time && !cancellationToken.IsCancellationRequested)
                    {
                        checkerSet.Clear();

                        var observedCount = 0;

                        foreach (var pair in dic)
                        {
                            if (!allKeys.Contains(pair.Key))
                                throw new Exception("Enumeration returned unknown key!");

                            if (!checkerSet.Add(pair.Key))
                                throw new Exception("Enumeration returned a duplicate key!");

                            if (pair.Value != pair.Key.ToString())
                                throw new Exception("Enumeration returned wrong value for key!");

                            observedCount++;
                        }

                        if (observedCount < keys.Length / 2)
                            throw new Exception("Enumeration certainly did not return existing elements!");
                    }
                }
                catch (Exception error)
                {
                    Console.Out.WriteLine(error);

                    throw new Exception("Error in enumerator!", error);
                }
            });

            tasks.Add(task);
        }

        private void LaunchStrictEnumerator(TimeSpan time, CancellationToken cancellationToken)
        {
            var task = Task.Run(() =>
            {
                var watch = Stopwatch.StartNew();

                var checkerSet = new HashSet<Guid>();

                try
                {
                    while (watch.Elapsed < time && !cancellationToken.IsCancellationRequested)
                    {
                        checkerSet.Clear();

                        var observedCount = 0;

                        foreach (var pair in dic)
                        {
                            if (!allKeys.Contains(pair.Key))
                                throw new Exception("Enumeration returned unknown key!");

                            if (!checkerSet.Add(pair.Key))
                                throw new Exception("Enumeration returned a duplicate key!");

                            if (pair.Value != pair.Key.ToString())
                                throw new Exception("Enumeration returned wrong value for key!");

                            observedCount++;
                        }

                        if (observedCount != keys.Length)
                            throw new Exception("Enumeration returned wrong count of elements!");
                    }
                }
                catch (Exception error)
                {
                    Console.Out.WriteLine(error);

                    throw new Exception("Error in enumerator!", error);
                }
            });

            tasks.Add(task);
        }
    }
}