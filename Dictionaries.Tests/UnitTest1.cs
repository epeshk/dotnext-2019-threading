using System;
using System.Collections.Generic;
using System.Linq;
using FluentAssertions;
using NUnit.Framework;

namespace Dictionaries.Tests
{
    [TestFixture]
    internal class StripedDictionaryTests
    {
        private StripedDictionary<Guid, string, FastGuidComparer> dic;

        [SetUp]
        public void TestSetup()
        {
            dic = new StripedDictionary<Guid, string, FastGuidComparer>();;
        }

        [Test]
        public void Add_should_not_fail_when_adding_new_element()
        {
            for (int i = 0; i < 100 * 1000; i++)
            {
                dic.Add(Guid.NewGuid(), "value");
            }
        }

        [Test]
        public void Add_should_fail_when_overwriting_existing_elements()
        {
            for (int i = 0; i < 1000; i++)
            {
                var key = Guid.NewGuid();

                Action action = () => dic.Add(key, "value");

                action();

                action.Should().Throw<ArgumentException>();
            }
        }

        [Test]
        public void TryAdd_should_return_false_when_trying_to_overwrite_existing_elements()
        {
            for (int i = 0; i < 1000; i++)
            {
                var key = Guid.NewGuid();

                dic.Add(key, "value");

                dic.TryAdd(key, "value2").Should().BeFalse();
                dic.TryAdd(key, "value2").Should().BeFalse();
            }
        }

        [Test]
        public void Add_should_increase_count_when_adding_new_elements()
        {
            dic.Count.Should().Be(0);

            for (int i = 0; i < 100 * 1000; i++)
            {
                dic.Add(Guid.NewGuid(), "value");

                dic.Count.Should().Be(i + 1);
            }
        }

        [Test]
        public void TryAdd_should_not_increase_count_when_trying_to_overwrite_existing_elements()
        {
            for (int i = 0; i < 1000; i++)
            {
                var key = Guid.NewGuid();

                dic.Add(key, "value");

                dic.TryAdd(key, "value2");
                dic.TryAdd(key, "value2");

                dic.Count.Should().Be(i + 1);
            }
        }

        [Test]
        public void TryAdd_should_not_modify_value_when_trying_to_overwrite_existing_elements()
        {
            for (int i = 0; i < 1000; i++)
            {
                var key = Guid.NewGuid();

                dic.Add(key, "value");

                dic.TryAdd(key, "value2");
                dic.TryAdd(key, "value2");

                dic[key].Should().Be("value");
            }
        }

        [Test]
        public void Set_should_not_fail_when_adding_new_element()
        {
            for (int i = 0; i < 100 * 1000; i++)
            {
                dic.Set(Guid.NewGuid(), "value");
            }
        }

        [Test]
        public void Set_should_increase_count_when_adding_new_elements()
        {
            dic.Count.Should().Be(0);

            for (int i = 0; i < 100 * 1000; i++)
            {
                dic.Set(Guid.NewGuid(), "value");

                dic.Count.Should().Be(i + 1);
            }
        }

        [Test]
        public void Set_should_not_fail_when_overwriting_existing_elements()
        {
            for (int i = 0; i < 100 * 1000; i++)
            {
                var key = Guid.NewGuid();

                dic.Set(key, "value1");
                dic.Set(key, "value2");
            }
        }

        [Test]
        public void Set_should_not_increase_count_when_overwriting_existing_elements()
        {
            for (int i = 0; i < 100 * 1000; i++)
            {
                var key = Guid.NewGuid();

                dic.Set(key, "value1");

                var countBefore = dic.Count;

                dic.Set(key, "value2");
                dic.Set(key, "value3");

                dic.Count.Should().Be(countBefore);
            }
        }

        [Test]
        public void Remove_should_return_true_when_removing_existing_key()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());
            }

            foreach (var key in keys)
            {
                dic.Remove(key).Should().BeTrue();
            }
        }

        [Test]
        public void Remove_should_decrement_count_when_removing_existing_key()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());
            }

            var expectedCount = dic.Count;

            foreach (var key in keys)
            {
                dic.Remove(key).Should().BeTrue();

                dic.Count.Should().Be(--expectedCount);
            }
        }

        [Test]
        public void Remove_should_return_false_when_removing_non_existing_key()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());
            }

            var expectedCount = dic.Count;

            for (int i = 0; i < 100; i++)
            {
                dic.Remove(Guid.NewGuid()).Should().BeFalse();
            }

            dic.Count.Should().Be(expectedCount);

            foreach (var key in keys)
            {
                dic.Remove(key);

                dic.Count.Should().Be(--expectedCount);

                dic.Remove(key).Should().BeFalse();
                dic.Remove(key).Should().BeFalse();

                dic.Count.Should().Be(expectedCount);
            }
        }

        [Test]
        public void Remove_should_not_decrement_count_when_removing_non_existing_element()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());
            }

            var expectedCount = dic.Count;

            for (int i = 0; i < 100; i++)
            {
                dic.Remove(Guid.NewGuid()).Should().BeFalse();

                dic.Count.Should().Be(expectedCount);
            }

            foreach (var key in keys)
            {
                dic.Remove(key);

                dic.Count.Should().Be(--expectedCount);

                dic.Remove(key).Should().BeFalse();
                dic.Remove(key).Should().BeFalse();

                dic.Count.Should().Be(expectedCount);
            }
        }

        [Test]
        public void TryGetValue_should_return_true_for_added_keys()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());

                dic.TryGetValue(key, out _).Should().BeTrue();
            }

            foreach (var key in keys)
            {
                dic.TryGetValue(key, out _).Should().BeTrue();
            }
        }

        [Test]
        public void TryGetValue_should_return_correct_values_for_existing_keys()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());

                dic.TryGetValue(key, out var value);

                value.Should().Be(key.ToString());
            }

            foreach (var key in keys)
            {
                dic.TryGetValue(key, out var value);

                value.Should().Be(key.ToString());
            }
        }

        [Test]
        public void TryGetValue_should_return_updated_values_after_overwrites()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());

                dic.TryGetValue(key, out var value);

                value.Should().Be(key.ToString());

                for (int i = 1; i <= 3; i++)
                {
                    dic.Set(key, key.ToString() + i);

                    dic.TryGetValue(key, out value);

                    value.Should().Be(key.ToString() + i);
                }
            }

            foreach (var key in keys)
            {
                dic.TryGetValue(key, out var value);

                value.Should().Be(key + "3");
            }
        }

        [Test]
        public void TryGetValue_should_return_false_for_never_added_keys()
        {
            var keys = Enumerable.Range(0, 100 * 000).Select(_ => Guid.NewGuid()).ToArray();

            foreach (var key in keys)
            {
                dic.Add(key, key.ToString());
            }

            for (int i = 0; i < 1000; i++)
            {
                dic.TryGetValue(Guid.NewGuid(), out _).Should().BeFalse();
            }
        }

        [Test]
        public void Enumeration_should_return_correct_pairs()
        {
            var keys = Enumerable.Range(0, 1000).Select(_ => Guid.NewGuid()).ToArray();

            for (int i = 0; i < 10; i++)
            {
                foreach (var key in keys.Skip(i * 100).Take(100))
                {
                    dic.Add(key, key.ToString());
                }

                var expected = keys.Take((i + 1) * 100)
                    .Select(key => (key, key.ToString()))
                    .OrderBy(x => x.key);

                var dictPairs = dic.Select(x => (x.Key, x.Value)).OrderBy(x => x.Key);

                expected.Should().BeEquivalentTo(dictPairs, x => x.WithStrictOrdering());
            }
        }

        [Test]
        public void Enumeration_should_work_on_empty_dictionary()
        {
            dic.Should().BeEmpty();
        }

        [Test]
        public void Should_support_null_values()
        {
            var key = Guid.NewGuid();

            dic.Add(key, null);

            dic.TryGetValue(key, out var value).Should().BeTrue();

            value.Should().BeNull();
        }

        [Test]
        public void Should_initialize_with_given_capacity_so_that_no_further_resize_is_needed()
        {
            var itemsCount = 150 * 1000;

            dic = new StripedDictionary<Guid, string, FastGuidComparer>(itemsCount);

            var capacityBefore = dic.Capacity;
            var segmentsCountBefore = dic.SegmentsCount;

            Console.Out.WriteLine($"Capacity before = {capacityBefore}");
            Console.Out.WriteLine($"Segments before = {segmentsCountBefore}");

            for (int i = 0; i < itemsCount; i++)
            {
                dic.Add(Guid.NewGuid(), string.Empty);
            }

            Console.Out.WriteLine($"Capacity after = {dic.Capacity}");
            Console.Out.WriteLine($"Segments after = {dic.SegmentsCount}");

            dic.Count.Should().Be(itemsCount);
            dic.Capacity.Should().Be(capacityBefore);
            dic.SegmentsCount.Should().Be(segmentsCountBefore);
            dic.HasArraysInLOH.Should().BeFalse();
        }

        [Test]
        public void Should_allow_to_remove_elements_during_enumeration()
        {
            for (int i = 0; i < 1000; i++)
            {
                dic.Add(Guid.NewGuid(), string.Empty);
            }

            foreach (var pair in dic)
            {
                dic.Remove(pair.Key);
                dic.ContainsKey(pair.Key).Should().BeFalse();
            }

            dic.Count.Should().Be(0);
        }

        [Test]
        public void Should_shrink_after_removing_a_lot_of_elements()
        {
            var itemsCount = 150 * 1000;

            dic = new StripedDictionary<Guid, string, FastGuidComparer>(itemsCount);

            var capacityBefore = dic.Capacity;
            var segmentsCountBefore = dic.SegmentsCount;

            Console.Out.WriteLine($"Capacity before = {capacityBefore}");
            Console.Out.WriteLine($"Segments before = {segmentsCountBefore}");

            for (int i = 0; i < itemsCount; i++)
            {
                dic.Add(Guid.NewGuid(), string.Empty);
            }

            var capacityAfterFilling = dic.Capacity;
            var segmentsCountAfterFilling = dic.SegmentsCount;

            Console.Out.WriteLine($"Capacity after filling = {capacityAfterFilling}");
            Console.Out.WriteLine($"Segments after filling = {segmentsCountAfterFilling}");

            foreach (var pair in dic)
            {
                dic.Remove(pair.Key);
            }

            Console.Out.WriteLine($"Capacity after cleaning = {dic.Capacity}");
            Console.Out.WriteLine($"Segments after cleaning = {dic.SegmentsCount}");

            dic.Capacity.Should().BeLessThan(capacityAfterFilling);
            dic.SegmentsCount.Should().BeLessThan(segmentsCountAfterFilling);
            dic.HasArraysInLOH.Should().BeFalse();
        }

        [Test]
        public void Indexer_should_throw_when_key_does_not_exist()
        {
            Action action = () => dic[Guid.NewGuid()].GetHashCode();

            action.Should().Throw<KeyNotFoundException>();
        }
    }
}