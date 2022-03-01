/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ESPkMeansLib.Helpers;

/// <summary>
/// Minimal wrapper logic for Dictionary<![CDATA[<]]><typeparamref name="TKey"/>, List<![CDATA[<]]>int>>
/// like semantic (items can only be added to a key, not removed).
/// Important: only non-negative numbers can be added to a list.
/// Actual List object will only be created if there is more than one element for that key.
/// Single elements are directly stored as complement in the underlying dictionary.
/// Reduces GC pressure and saves memory if the percentage of single-entry lists is significant.
/// </summary>
/// <typeparam name="TKey"></typeparam>
public class DictListInt<TKey> where TKey : notnull
{

    private readonly Dictionary<TKey, int> _dict = new();
    private List<int>[] _entries = new List<int>[4];
    private int _entriesCount;
    private int _availableCount;
    public int Count => _dict.Count;
    public int EntriesCount => _entriesCount;

    internal int AvailableCount => _availableCount;
    //public int TotalEntriesCount => _totalCount;
    public ICollection<TKey> Keys => _dict.Keys;

    /// <summary>
    /// Clear the dictionary, but keep references to the already instantiated lists as cache.
    /// </summary>
    public void Clear()
    {
        _dict.Clear();
        for (var i = 0; i < _entriesCount; i++)
        {
            //Clear() on a list without references is very efficient (will just set counter to 0)
            _entries.DangerousGetReferenceAt(i).Clear();
        }
        _entriesCount = 0;
    }

    /// <summary>
    /// Add the provided non-negative value to the list of the specified key (new entry will be added
    /// if key does not exist yet).
    /// </summary>
    /// <param name="key"></param>
    /// <param name="val">Value to add, must be non-negative</param>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    public void AddToList(TKey key, int val)
    {
        if (val < 0)
            throw new ArgumentOutOfRangeException(nameof(val), "data structure only supports non-negative values");
        //_totalCount++;
        ref int ptr = ref CollectionsMarshal.GetValueRefOrAddDefault(_dict, key, out var exists);
        if (!exists)
        {
            //save entry as complement
            ptr = ~val;
            return;
        }

        if (ptr >= 0)
        {
            //we have reference to list
            _entries.DangerousGetReferenceAt(ptr).Add(val);
            return;
        }

        //first entry was saved as complement
        var firstVal = ~ptr;
        var l = InitList(ref ptr);
        l.Add(firstVal);
        l.Add(val);
    }

    /// <summary>
    /// Bulk add list of values to the list of the specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="values"></param>
    public void AddRange(TKey key, IList<int> values)
    {
        if (values.Count == 0)
            return;
        if (values.Count == 1)
        {
            AddToList(key, values[0]);
            return;
        }

        ref int ptr = ref CollectionsMarshal.GetValueRefOrAddDefault(_dict, key, out var exists);

        if (exists && ptr >= 0)
        {
            //we have reference to list
            _entries.DangerousGetReferenceAt(ptr).AddRange(values);
            return;
        }
        
        var prevPtr = ptr;
        var l = InitList(ref ptr, values.Count + 1);
        if(exists)
            l.Add(~prevPtr);//first entry was saved as complement
        l.AddRange(values);
    }


    /// <summary>
    /// Ensure that list of specified key exists and has specified capacity.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="capacity"></param>
    public void EnsureListCapacity(TKey key, int capacity)
    {
        ref int ptr = ref CollectionsMarshal.GetValueRefOrAddDefault(_dict, key, out var exists);

        if (exists && ptr >= 0)
        {
            //we have reference to list
            _entries.DangerousGetReferenceAt(ptr).EnsureCapacity(capacity);
            return;
        }

        var prevPtr = ptr;
        var l = InitList(ref ptr, capacity);
        if (exists)
            l.Add(~prevPtr);//first entry was saved as complements
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private List<int> InitList(ref int entriesPtr, int listCapacity = 4)
    {
        entriesPtr = _entriesCount;
        _entriesCount++;
        List<int> l;
        if (entriesPtr < _availableCount)
            l = _entries.DangerousGetReferenceAt(entriesPtr);
        else
        {
            EnsureCapacity(_entriesCount);
            l = new List<int>(listCapacity);
            _entries[entriesPtr] = l;
            _availableCount = _entriesCount;
        }

        return l;
    }

    /// <summary>
    /// Ensure capacity of underlying dictionary that stores all keys
    /// </summary>
    /// <param name="capacity"></param>
    public void EnsureDictCapacity(int capacity)
    {
        _dict.EnsureCapacity(capacity);
    }



    internal void EnsureCapacity(int capacity)
    {
        if (_entries.Length >= capacity)
            return;
        var newcapacity = _entries.Length * 2;
        if ((uint)newcapacity > Array.MaxLength) newcapacity = Array.MaxLength;
        if (newcapacity < capacity) newcapacity = capacity;
        Array.Resize(ref _entries, newcapacity);
    }

    /// <summary>
    /// Get list of specified key. Will return empty list if key could not be found.
    /// </summary>
    /// <param name="key"></param>
    /// <returns></returns>
    public IList<int> this[TKey key] => TryGetValue(key, out var l) ? l : Array.Empty<int>();

    /// <summary>
    /// Try to get list of specified key.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="list"></param>
    /// <returns>Whether the specified list could be found and contains at least one entry</returns>
    [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
    public bool TryGetValue(TKey key, out IList<int> list)
    {
        if (!_dict.TryGetValue(key, out var p))
        {
            list = Array.Empty<int>();
            return false;
        }
        list = p < 0 ? new[] {~p } : _entries.DangerousGetReferenceAt(p);
        return true;
                
            
    }
}