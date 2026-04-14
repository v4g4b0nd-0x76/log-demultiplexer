use rayon::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::hash_map::DefaultHasher,
    fs,
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::Arc,
};

use crate::conf::ParsedBatch;

const PERSIST_DIR: &str = "./persist";

struct RingBuffer<V: Clone> {
    data: Box<[Option<V>]>,
    head: usize,
    len: usize,
    cap: usize,
}

impl<V: Clone> RingBuffer<V> {
    fn new(cap: usize) -> Self {
        Self {
            data: vec![None; cap].into_boxed_slice(),
            head: 0,
            len: 0,
            cap,
        }
    }

    fn push(&mut self, val: V) -> usize {
        if self.len == self.cap {
            let idx = self.head;
            self.data[idx] = Some(val);
            self.head = (self.head + 1) % self.cap;
            idx
        } else {
            let idx = (self.head + self.len) % self.cap;
            self.data[idx] = Some(val);
            self.len += 1;
            idx
        }
    }

    fn remove(&mut self, logical_idx: usize) {
        if logical_idx >= self.len {
            return;
        }
        for i in logical_idx..self.len - 1 {
            let cur = (self.head + i) % self.cap;
            let next = (self.head + i + 1) % self.cap;
            self.data[cur] = self.data[next].take();
        }
        let last = (self.head + self.len - 1) % self.cap;
        self.data[last] = None;
        self.len -= 1;
    }

    fn insert_at_empty(&mut self, val: V) -> Option<usize> {
        for i in 0..self.cap {
            if self.data[i].is_none() {
                self.data[i] = Some(val);
                if self.len < self.cap {
                    self.len += 1;
                }
                return Some(i);
            }
        }
        None
    }

    fn get(&self, logical_idx: usize) -> Option<&V> {
        if logical_idx >= self.len {
            return None;
        }
        self.data[(self.head + logical_idx) % self.cap].as_ref()
    }

    fn iter_ordered(&self) -> impl Iterator<Item = (usize, &V)> {
        (0..self.len).map(move |i| {
            let phys = (self.head + i) % self.cap;
            (i, self.data[phys].as_ref().unwrap())
        })
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }
}

struct Slot<K: Clone, V: Clone> {
    key: Option<K>,
    buf: Option<RingBuffer<V>>,
    ring_cap: usize,
}

impl<K: Clone, V: Clone> Slot<K, V> {
    fn empty(ring_cap: usize) -> Self {
        Self {
            key: None,
            buf: None,
            ring_cap,
        }
    }

    fn init(&mut self, key: K) {
        self.key = Some(key);
        self.buf = Some(RingBuffer::new(self.ring_cap));
    }

    fn buf_mut(&mut self) -> &mut RingBuffer<V> {
        self.buf.as_mut().unwrap()
    }

    fn buf_ref(&self) -> &RingBuffer<V> {
        self.buf.as_ref().unwrap()
    }
}

pub struct RingMap<K: Clone + Hash + Eq, V: Clone> {
    slots: Box<[Slot<K, V>]>,
    n_slots: usize,
    ring_cap: usize,
}

impl<K: Clone + Hash + Eq, V: Clone> RingMap<K, V> {
    pub fn new(n_slots: usize, ring_cap: usize) -> Self {
        let slots = (0..n_slots)
            .map(|_| Slot::empty(ring_cap))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            slots,
            n_slots,
            ring_cap,
        }
    }

    fn base_idx(&self, key: &K) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.n_slots
    }

    fn find_slot(&self, key: &K) -> Option<usize> {
        let base = self.base_idx(key);
        for i in 0..self.n_slots {
            let idx = (base + i) % self.n_slots;
            match &self.slots[idx].key {
                Some(k) if k == key => return Some(idx),
                None => return None,
                _ => {}
            }
        }
        None
    }

    fn find_or_create_slot(&mut self, key: &K) -> usize {
        let base = self.base_idx(key);
        for i in 0..self.n_slots {
            let idx = (base + i) % self.n_slots;
            match &self.slots[idx].key {
                Some(k) if k == key => return idx,
                None => {
                    self.slots[idx].init(key.clone());
                    return idx;
                }
                _ => {}
            }
        }
        panic!("RingMap full ({} slots)", self.n_slots);
    }

    pub fn push(&mut self, key: K, val: V) -> usize {
        let slot = self.find_or_create_slot(&key);
        self.slots[slot].buf_mut().push(val)
    }

    pub fn remove_entry(&mut self, key: &K, logical_idx: usize) {
        if let Some(slot) = self.find_slot(key) {
            self.slots[slot].buf_mut().remove(logical_idx);
        }
    }

    pub fn insert_at_empty(&mut self, key: K, val: V) -> Option<usize> {
        let slot = self.find_or_create_slot(&key);
        self.slots[slot].buf_mut().insert_at_empty(val)
    }

    pub fn get(&self, key: &K, logical_idx: usize) -> Option<&V> {
        self.find_slot(key)
            .and_then(|s| self.slots[s].buf_ref().get(logical_idx))
    }

    pub fn iter_ordered(&self, key: &K) -> impl Iterator<Item = (usize, &V)> {
        self.find_slot(key)
            .map(|s| self.slots[s].buf_ref())
            .into_iter()
            .flat_map(|b| b.iter_ordered())
    }

    pub fn is_empty(&self, key: &K) -> bool {
        self.find_slot(key)
            .map(|s| self.slots[s].buf_ref().is_empty())
            .unwrap_or(true)
    }
}

pub fn flush_to_disk<V>(map: &RingMap<String, V>, base_dir: &str) -> std::io::Result<()>
where
    V: Serialize + Clone,
{
    fs::create_dir_all(base_dir)?;
    let cfg = bincode::config::standard();
    for slot in map.slots.iter() {
        let Some(key) = &slot.key else { continue };
        let Some(buf) = &slot.buf else { continue };
        if buf.is_empty() {
            continue;
        }
        let owned: Vec<V> = buf.iter_ordered().map(|(_, v)| v.clone()).collect();
        let bytes = bincode::serde::encode_to_vec(&owned, cfg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(PathBuf::from(base_dir).join(format!("{}.bin", key)), bytes)?;
    }
    Ok(())
}

pub fn load_from_disk<V>(base_dir: &str) -> std::io::Result<Vec<(String, Vec<V>)>>
where
    V: DeserializeOwned + Clone + Send,
{
    let entries: Vec<_> = fs::read_dir(base_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |x| x == "bin"))
        .collect();

    Ok(entries
        .par_iter()
        .filter_map(|entry| {
            let path = entry.path();
            let key = path.file_stem()?.to_str()?.to_string();
            let bytes = fs::read(&path).ok()?;
            let cfg = bincode::config::standard();
            let (vals, _): (Vec<V>, _) = bincode::serde::decode_from_slice(&bytes, cfg).ok()?;
            Some((key, vals))
        })
        .collect())
}

pub struct ConsumerPersistBuffer {
    ring: RingBuffer<Arc<ParsedBatch>>,
    persist_dir: PathBuf,
}

impl ConsumerPersistBuffer {
    fn new_inner(consumer_key: &str, max_batch: usize) -> Self {
        let persist_dir = PathBuf::from(PERSIST_DIR).join(sanitize_key(consumer_key));
        Self {
            ring: RingBuffer::new(max_batch),
            persist_dir,
        }
    }

    pub fn new(consumer_key: &str, max_batch: usize) -> Self {
        Self::new_inner(consumer_key, max_batch)
    }

    pub fn persist_and_push(&mut self, batch: Arc<ParsedBatch>) -> std::io::Result<usize> {
        fs::create_dir_all(&self.persist_dir)?;

        let next_logical = if self.ring.len == self.ring.cap {
            let evicted_path = self.entry_path(self.ring.head);
            let _ = fs::remove_file(&evicted_path);
            self.ring.head
        } else {
            self.ring.len
        };

        let path = self.persist_dir.join(format!("{}.bin", next_logical));
        let cfg = bincode::config::standard();
        let bytes = bincode::serde::encode_to_vec(batch.as_ref(), cfg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(&path, bytes)?;

        let phys = self.ring.push(Arc::clone(&batch));
        Ok(phys)
    }

    pub fn confirm_delivered(&mut self, logical_idx: usize) {
        let _ = fs::remove_file(self.entry_path(logical_idx));
        self.ring.remove(logical_idx);
    }

    pub fn replay_all(&self) -> Vec<(usize, Arc<ParsedBatch>)> {
        self.ring
            .iter_ordered()
            .map(|(i, b)| (i, Arc::clone(b)))
            .collect()
    }

    pub fn flush_bulk(&self, base_dir: &str) -> std::io::Result<()> {
        if self.ring.is_empty() {
            return Ok(());
        }
        fs::create_dir_all(base_dir)?;
        let key = self
            .persist_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        let cfg = bincode::config::standard();
        let owned: Vec<&ParsedBatch> = self.ring.iter_ordered().map(|(_, b)| b.as_ref()).collect();
        let bytes = bincode::serde::encode_to_vec(&owned, cfg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        fs::write(PathBuf::from(base_dir).join(format!("{}.bin", key)), bytes)
    }

    fn entry_path(&self, slot: usize) -> PathBuf {
        self.persist_dir.join(format!("{}.bin", slot))
    }

    pub fn new_in(consumer_key: &str, max_batch: usize, base: &std::path::Path) -> Self {
        let persist_dir = base.join(sanitize_key(consumer_key));
        Self {
            ring: RingBuffer::new(max_batch),
            persist_dir,
        }
    }

    pub fn persist_dir(&self) -> &std::path::Path {
        &self.persist_dir
    }

    pub fn entry_path_pub(&self, slot: usize) -> std::path::PathBuf {
        self.entry_path(slot)
    }
}

pub fn load_consumer_buffer_in(
    consumer_key: &str,
    max_batch: usize,
    base: &std::path::Path,
) -> ConsumerPersistBuffer {
    let mut buf = ConsumerPersistBuffer::new_in(consumer_key, max_batch, base);
    if !buf.persist_dir.exists() {
        return buf;
    }
    let mut entries: Vec<(usize, std::path::PathBuf)> = std::fs::read_dir(&buf.persist_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |x| x == "bin"))
        .filter_map(|e| {
            let path = e.path();
            let idx = path.file_stem()?.to_str()?.parse::<usize>().ok()?;
            Some((idx, path))
        })
        .collect();
    entries.sort_by_key(|(idx, _)| *idx);
    let loaded: Vec<ParsedBatch> = entries
        .par_iter()
        .filter_map(|(_, path)| {
            let bytes = std::fs::read(path).ok()?;
            let (batch, _): (ParsedBatch, _) =
                bincode::serde::decode_from_slice(&bytes, bincode::config::standard()).ok()?;
            Some(batch)
        })
        .collect();
    for batch in loaded {
        buf.ring.push(Arc::new(batch));
    }
    buf
}
pub fn load_consumer_buffer(consumer_key: &str, max_batch: usize) -> ConsumerPersistBuffer {
    let mut buf = ConsumerPersistBuffer::new_inner(consumer_key, max_batch);

    if !buf.persist_dir.exists() {
        return buf;
    }

    let mut entries: Vec<(usize, PathBuf)> = fs::read_dir(&buf.persist_dir)
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |x| x == "bin"))
        .filter_map(|e| {
            let path = e.path();
            let idx = path.file_stem()?.to_str()?.parse::<usize>().ok()?;
            Some((idx, path))
        })
        .collect();

    entries.sort_by_key(|(idx, _)| *idx);

    let loaded: Vec<(usize, ParsedBatch)> = entries
        .par_iter()
        .filter_map(|(idx, path)| {
            let bytes = fs::read(path).ok()?;
            let cfg = bincode::config::standard();
            let (batch, _): (ParsedBatch, _) =
                bincode::serde::decode_from_slice(&bytes, cfg).ok()?;
            Some((*idx, batch))
        })
        .collect();

    for (_, batch) in loaded {
        buf.ring.push(Arc::new(batch));
    }

    buf
}

fn sanitize_key(key: &str) -> String {
    key.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn batch(items: &[&str]) -> Arc<ParsedBatch> {
        Arc::new(ParsedBatch::Str(
            items.iter().map(|s| s.to_string()).collect(),
        ))
    }

    fn make_buf(dir: &TempDir, key: &str, cap: usize) -> ConsumerPersistBuffer {
        ConsumerPersistBuffer::new_in(key, cap, dir.path())
    }

    #[test]
    fn ring_push_returns_sequential_physical_slots() {
        let mut r: RingBuffer<i32> = RingBuffer::new(4);
        assert_eq!(r.push(10), 0);
        assert_eq!(r.push(20), 1);
        assert_eq!(r.push(30), 2);
    }

    #[test]
    fn ring_iter_ordered_reflects_insertion_order() {
        let mut r: RingBuffer<i32> = RingBuffer::new(4);
        r.push(1);
        r.push(2);
        r.push(3);
        let vals: Vec<i32> = r.iter_ordered().map(|(_, v)| *v).collect();
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn ring_get_returns_correct_logical_entry() {
        let mut r: RingBuffer<i32> = RingBuffer::new(4);
        r.push(10);
        r.push(20);
        r.push(30);
        assert_eq!(r.get(0), Some(&10));
        assert_eq!(r.get(1), Some(&20));
        assert_eq!(r.get(2), Some(&30));
        assert_eq!(r.get(3), None);
    }

    #[test]
    fn ring_remove_shifts_remaining_entries() {
        let mut r: RingBuffer<i32> = RingBuffer::new(4);
        r.push(1);
        r.push(2);
        r.push(3);
        r.remove(1);
        let vals: Vec<i32> = r.iter_ordered().map(|(_, v)| *v).collect();
        assert_eq!(vals, vec![1, 3]);
    }

    #[test]
    fn ring_wraps_and_evicts_oldest_when_full() {
        let mut r: RingBuffer<i32> = RingBuffer::new(3);
        r.push(1);
        r.push(2);
        r.push(3);
        r.push(4);
        let vals: Vec<i32> = r.iter_ordered().map(|(_, v)| *v).collect();
        assert_eq!(vals, vec![2, 3, 4]);
    }

    #[test]
    fn ring_is_empty_initially() {
        let r: RingBuffer<i32> = RingBuffer::new(4);
        assert!(r.is_empty());
    }

    #[test]
    fn ring_not_empty_after_push() {
        let mut r: RingBuffer<i32> = RingBuffer::new(4);
        r.push(1);
        assert!(!r.is_empty());
    }

    #[test]
    fn ring_insert_at_empty_fills_first_none_slot() {
        let mut r: RingBuffer<i32> = RingBuffer::new(4);
        let idx = r.insert_at_empty(99);
        assert!(idx.is_some());
        assert_eq!(r.get(0), Some(&99));
    }

    #[test]
    fn ring_insert_at_empty_returns_none_when_full() {
        let mut r: RingBuffer<i32> = RingBuffer::new(2);
        r.push(1);
        r.push(2);
        assert!(r.insert_at_empty(3).is_none());
    }

    #[test]
    fn ringmap_push_and_get() {
        let mut m: RingMap<String, i32> = RingMap::new(8, 4);
        let idx = m.push("a".to_string(), 42);
        assert_eq!(m.get(&"a".to_string(), idx), Some(&42));
    }

    #[test]
    fn ringmap_separate_keys_are_independent() {
        let mut m: RingMap<String, i32> = RingMap::new(8, 4);
        m.push("a".to_string(), 1);
        m.push("b".to_string(), 2);
        assert_eq!(m.get(&"a".to_string(), 0), Some(&1));
        assert_eq!(m.get(&"b".to_string(), 0), Some(&2));
    }

    #[test]
    fn ringmap_remove_entry_deletes_value() {
        let mut m: RingMap<String, i32> = RingMap::new(8, 4);
        m.push("a".to_string(), 10);
        m.push("a".to_string(), 20);
        m.remove_entry(&"a".to_string(), 0);
        let vals: Vec<i32> = m.iter_ordered(&"a".to_string()).map(|(_, v)| *v).collect();
        assert_eq!(vals, vec![20]);
    }

    #[test]
    fn ringmap_iter_ordered_returns_insertion_order() {
        let mut m: RingMap<String, i32> = RingMap::new(8, 4);
        m.push("k".to_string(), 1);
        m.push("k".to_string(), 2);
        m.push("k".to_string(), 3);
        let vals: Vec<i32> = m.iter_ordered(&"k".to_string()).map(|(_, v)| *v).collect();
        assert_eq!(vals, vec![1, 2, 3]);
    }

    #[test]
    fn ringmap_is_empty_for_unknown_key() {
        let m: RingMap<String, i32> = RingMap::new(8, 4);
        assert!(m.is_empty(&"missing".to_string()));
    }

    #[test]
    #[should_panic]
    fn ringmap_panics_when_slots_exhausted() {
        let mut m: RingMap<String, i32> = RingMap::new(2, 4);
        m.push("a".to_string(), 1);
        m.push("b".to_string(), 2);
        m.push("c".to_string(), 3);
    }

    #[test]
    fn persist_and_push_writes_file_to_disk() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "consumer1", 4);

        let b = batch(&["hello"]);
        buf.persist_and_push(b).unwrap();

        let files: Vec<_> = std::fs::read_dir(buf.persist_dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn persist_and_push_returns_incrementing_logical_indices() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "consumer1", 4);

        let i0 = buf.persist_and_push(batch(&["a"])).unwrap();
        let i1 = buf.persist_and_push(batch(&["b"])).unwrap();
        let i2 = buf.persist_and_push(batch(&["c"])).unwrap();

        assert_eq!((i0, i1, i2), (0, 1, 2));
    }

    #[test]
    fn confirm_delivered_removes_file() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "consumer1", 4);

        let idx = buf.persist_and_push(batch(&["x"])).unwrap();
        buf.confirm_delivered(idx);

        let files: Vec<_> = std::fs::read_dir(buf.persist_dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(files.is_empty());
    }

    #[test]
    fn confirm_delivered_removes_from_ring() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "consumer1", 4);

        buf.persist_and_push(batch(&["a"])).unwrap();
        let idx = buf.persist_and_push(batch(&["b"])).unwrap();
        buf.persist_and_push(batch(&["c"])).unwrap();

        buf.confirm_delivered(idx);

        let replays = buf.replay_all();
        let contents: Vec<&str> = replays
            .iter()
            .flat_map(|(_, b)| match b.as_ref() {
                ParsedBatch::Str(v) => v.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                _ => vec![],
            })
            .collect();
        assert!(!contents.contains(&"b"));
        assert!(contents.contains(&"a"));
        assert!(contents.contains(&"c"));
    }

    #[test]
    fn replay_all_returns_all_unconfirmed_in_order() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "consumer1", 4);

        buf.persist_and_push(batch(&["first"])).unwrap();
        buf.persist_and_push(batch(&["second"])).unwrap();
        buf.persist_and_push(batch(&["third"])).unwrap();

        let replays = buf.replay_all();
        assert_eq!(replays.len(), 3);

        let contents: Vec<String> = replays
            .iter()
            .flat_map(|(_, b)| match b.as_ref() {
                ParsedBatch::Str(v) => v.clone(),
                _ => vec![],
            })
            .collect();
        assert_eq!(contents, vec!["first", "second", "third"]);
    }

    #[test]
    fn eviction_removes_oldest_file_when_full() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "evict", 2);

        let i0 = buf.persist_and_push(batch(&["oldest"])).unwrap();
        buf.persist_and_push(batch(&["middle"])).unwrap();

        let oldest_path = buf.entry_path_pub(i0);
        assert!(oldest_path.exists());

        buf.persist_and_push(batch(&["newest"])).unwrap();

        assert!(
            !oldest_path.exists(),
            "evicted entry file should be deleted"
        );
    }

    #[test]
    fn eviction_keeps_correct_entries_in_ring() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "evict2", 2);

        buf.persist_and_push(batch(&["a"])).unwrap();
        buf.persist_and_push(batch(&["b"])).unwrap();
        buf.persist_and_push(batch(&["c"])).unwrap();

        let replays = buf.replay_all();
        let contents: Vec<String> = replays
            .iter()
            .flat_map(|(_, b)| match b.as_ref() {
                ParsedBatch::Str(v) => v.clone(),
                _ => vec![],
            })
            .collect();
        assert!(!contents.contains(&"a".to_string()));
        assert!(contents.contains(&"b".to_string()));
        assert!(contents.contains(&"c".to_string()));
    }

    #[test]
    fn load_consumer_buffer_restores_persisted_entries() {
        let dir = TempDir::new().unwrap();
        let key = "boot-test";

        {
            let mut buf = make_buf(&dir, key, 4);
            buf.persist_and_push(batch(&["first"])).unwrap();
            buf.persist_and_push(batch(&["second"])).unwrap();
        }

        let restored = load_consumer_buffer_in(key, 4, dir.path());
        let replays = restored.replay_all();
        assert_eq!(replays.len(), 2);

        let contents: Vec<String> = replays
            .iter()
            .flat_map(|(_, b)| match b.as_ref() {
                ParsedBatch::Str(v) => v.clone(),
                _ => vec![],
            })
            .collect();
        assert_eq!(contents, vec!["first", "second"]);
    }

    #[test]
    fn load_consumer_buffer_empty_when_no_dir() {
        let dir = TempDir::new().unwrap();
        let restored = load_consumer_buffer_in("nonexistent-consumer", 4, dir.path());
        assert!(restored.replay_all().is_empty());
    }

    #[test]
    fn load_consumer_buffer_excludes_confirmed_entries() {
        let dir = TempDir::new().unwrap();
        let key = "partial-confirm";

        {
            let mut buf = make_buf(&dir, key, 4);
            buf.persist_and_push(batch(&["keep"])).unwrap();
            let idx = buf.persist_and_push(batch(&["drop"])).unwrap();
            buf.persist_and_push(batch(&["keep2"])).unwrap();
            buf.confirm_delivered(idx);
        }

        let restored = load_consumer_buffer_in(key, 4, dir.path());
        let replays = restored.replay_all();
        let contents: Vec<String> = replays
            .iter()
            .flat_map(|(_, b)| match b.as_ref() {
                ParsedBatch::Str(v) => v.clone(),
                _ => vec![],
            })
            .collect();

        assert!(!contents.contains(&"drop".to_string()));
        assert!(contents.contains(&"keep".to_string()));
        assert!(contents.contains(&"keep2".to_string()));
    }

    #[test]
    fn flush_bulk_writes_single_file() {
        let dir = TempDir::new().unwrap();
        let mut buf = make_buf(&dir, "bulk", 4);

        buf.persist_and_push(batch(&["x"])).unwrap();
        buf.persist_and_push(batch(&["y"])).unwrap();

        let bulk_dir = TempDir::new().unwrap();
        buf.flush_bulk(bulk_dir.path().to_str().unwrap()).unwrap();

        let files: Vec<_> = std::fs::read_dir(bulk_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |x| x == "bin"))
            .collect();
        assert_eq!(files.len(), 1);
    }

    #[test]
    fn flush_bulk_skips_empty_buffer() {
        let dir = TempDir::new().unwrap();
        let buf = make_buf(&dir, "empty-bulk", 4);

        let bulk_dir = TempDir::new().unwrap();
        buf.flush_bulk(bulk_dir.path().to_str().unwrap()).unwrap();

        let files: Vec<_> = std::fs::read_dir(bulk_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert!(files.is_empty());
    }
}
