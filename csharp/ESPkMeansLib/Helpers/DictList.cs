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

        private readonly Dictionary<TKey, (TValue entry, List<TValue>? list)> _dict = new();
        private List<TValue>?[] _entries = new List<TValue>[2];
        private int _entriesCount;
        private int _availableCount;

        public int Count => _dict.Count;
        public int EntriesCount => _entriesCount;
        public ICollection<TKey> Keys => _dict.Keys;

        /// <summary>
        /// Clear entries, but do not throw away already instantiated lists.
        /// </summary>
        public void Clear()
        {
            _dict.Clear();
            for (var i = 0; i < _entriesCount; i++)
            {
                //Clear() on a list without references is very efficient (will just set counter to 0)
                _entries.DangerousGetReferenceAt(i)!.Clear();
            }
            _entriesCount = 0;
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
            ref (TValue entry, List<TValue>? list) ptr = ref CollectionsMarshal.GetValueRefOrAddDefault(_dict, key, out var exists);
            if (!exists)
            {
                ptr = (val, null);
                return;
            }

            if (ptr.list != null)
            {
                ptr.list.Add(val);
                return;
            }

            List<TValue> list;
            var idx = _entriesCount;
            _entriesCount++;
            if (idx < _availableCount)
            {
                list = _entries.DangerousGetReferenceAt(idx)!;
            }
            else
            {
                EnsureCapacity(_entriesCount);
                list = new List<TValue>(2);
                _availableCount = _entriesCount;
                _entries[idx] = list;
            }

            list.Add(ptr.entry);
            list.Add(val);
            ptr = (default, list);
        }


        internal void EnsureCapacity(int capacity)
        {
            if (_entries.Length >= capacity)
                return;
            Array.Resize(ref _entries, Math.Max(capacity, _entries.Length * 2));
        }

        /// <summary>
        /// Get list of specified key. Will return empty list if key could not be found.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public IList<TValue> this[TKey key] => TryGetValue(key, out var l) ? l : Array.Empty<TValue>();

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
                list = Array.Empty<TValue>();
                return false;
            }
            list = p.list != null ? p.list : new[] { p.entry };
            return true;
        }
    }
}
