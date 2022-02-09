using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace ESPkMeansLib.Helpers
{
    /// <summary>
    /// Minimal wrapper logic for Dictionary<![CDATA[<]]><typeparamref name="TKey"/>, List<![CDATA[<]]><typeparamref name="TValue"/>>>
    /// like semantic (items can only be added to a key, not removed).
    /// Actual List object will only be created if there is more than one element for that key.
    /// Reduces GC pressure and saves memory if the percentage of single-entry lists is significant.
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public class DictList<TKey, TValue> where TKey : notnull
    {

        private readonly Dictionary<TKey, int> _dict = new();
        private (bool set, TValue entry, List<TValue>? list)[] _entries = new (bool set, TValue entry, List<TValue>? list)[2];
        private int _entriesCount;

        public int Count => _dict.Count;
        public int EntriesCount => _entriesCount;
        public ICollection<TKey> Keys => _dict.Keys;

        /// <summary>
        /// Clear entries, but do not throw away already instantiated lists.
        /// </summary>
        public void Clear()
        {
            for (var i = _entriesCount - 1; i >= 0; i--)
            {
                var list = _entries[i].list;
                list?.Clear();
                _entries[i] = (false, default, list);
            }
        }

        /// <summary>
        /// Add item to the list of the specified key (new entry will be added
        /// if key does not exist yet)
        /// </summary>
        /// <param name="key"></param>
        /// <param name="val"></param>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public void AddToList(TKey key, TValue val)
        {
            ref int ptr = ref CollectionsMarshal.GetValueRefOrAddDefault(_dict, key, out var exists);
            if (!exists)
            {
                ptr = _entriesCount;
                _entriesCount++;
                EnsureCapacity(_entriesCount);
                _entries[ptr] = (true, val, null);
                return;
            }
            ref (bool set, TValue entry, List<TValue>? list) entry = ref _entries.DangerousGetReferenceAt(ptr);

            if (entry.set && entry.list == null)
            {
                var list = new List<TValue>(2) { entry.entry, val };
                entry = (true, default, list);
                return;
            }

            entry.list?.Add(val);
            if (!entry.set)
            {
                entry = (true, val, entry.list);
            }
        }


        internal void EnsureCapacity(int capacity)
        {
            if (_entries.Length >= capacity)
                return;
            Array.Resize(ref _entries, Math.Max(capacity, _entries.Length * 2));
        }

        /// <summary>
        /// Try to get list of specified key.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="list"></param>
        /// <returns>Whether the specified list could be found and contains at least one entry</returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(TKey key, out IList<TValue> list)
        {
            if (!_dict.TryGetValue(key, out var p))
            {
                goto NOT_FOUND;
            }
            ref (bool set, TValue entry, List<TValue>? list) entry = ref _entries.DangerousGetReferenceAt(p);
            if (!entry.set)
                goto NOT_FOUND;
            list = entry.list?.Count > 0 ? entry.list : new[] { entry.entry };
            return true;

        NOT_FOUND:
            list = Array.Empty<TValue>();
            return false;
        }
    }
}
