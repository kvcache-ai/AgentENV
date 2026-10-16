use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

#[derive(Debug)]
pub struct ReloadableIDAllocator {
    inner: Mutex<ReloadableIDAllocatorInner>,
}

#[derive(Debug)]
struct ReloadableIDAllocatorInner {
    next_id: u32,
    free_ids_lookup: HashSet<u32>,
    free_ids: VecDeque<u32>,
}

impl ReloadableIDAllocatorInner {
    fn insert_free_id(&mut self, id: u32) {
        if id >= self.next_id {
            return;
        }
        self.insert_known_free_id(id);
    }

    fn insert_known_free_id(&mut self, id: u32) {
        if self.free_ids_lookup.contains(&id) {
            return;
        }
        self.free_ids_lookup.insert(id);
        self.free_ids.push_back(id);
    }

    fn pop_free_id(&mut self) -> Option<u32> {
        if let Some(id) = self.free_ids.pop_front() {
            self.free_ids_lookup.remove(&id);
            Some(id)
        } else {
            None
        }
    }

    /// It will move `id` from free_ids if exists
    ///
    /// Return whether find `id` in existing free_ids
    fn remove_free_id(&mut self, id: u32) -> bool {
        if !self.free_ids_lookup.remove(&id) {
            // id is not in `free_ids_lookup`
            return false;
        }

        if let Some(pos) = self.free_ids.iter().position(|val| *val == id) {
            self.free_ids.remove(pos);
        }
        true
    }
}

impl ReloadableIDAllocator {
    pub fn new(initial_idx: u32) -> Self {
        Self {
            inner: Mutex::new(ReloadableIDAllocatorInner {
                next_id: initial_idx,
                free_ids: VecDeque::new(),
                free_ids_lookup: HashSet::new(),
            }),
        }
    }

    /// Tell allocator that `id` has been occupied. Typically
    /// used when reloading. If the `id` is not free, this function
    /// will panic.
    pub fn occupy_idx(&self, id: u32) {
        // id should not be too big
        assert!(id <= 100_000, "try occupy big id {id}");
        let mut inner = self.inner.lock().expect("inner lock poisoned");
        // remove id from free_ids
        if inner.remove_free_id(id) {
            return;
        }
        // then try to allocate `id`, from `next_id`
        for val in inner.next_id..id {
            inner.insert_known_free_id(val);
        }
        if inner.next_id <= id {
            inner.next_id = id + 1;
        }
    }

    pub fn allocate(&self) -> u32 {
        let mut inner = self.inner.lock().expect("inner lock poisoned");
        if let Some(val) = inner.pop_free_id() {
            val
        } else {
            let res = inner.next_id;
            inner.next_id += 1;
            res
        }
    }

    pub fn recycle(&self, id: u32) {
        self.inner
            .lock()
            .expect("inner lock poisoned")
            .insert_free_id(id);
    }

    /// Check whether `id` is free (has not been allocated).
    pub fn is_free(&self, id: u32) -> bool {
        let inner = self.inner.lock().expect("inner lock poisoned");
        if inner.free_ids_lookup.contains(&id) {
            return true;
        }
        id >= inner.next_id
    }

    /// Check whether `id` is occupied (has already been allocated).
    pub fn is_occupied(&self, id: u32) -> bool {
        !self.is_free(id)
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;

    use super::ReloadableIDAllocator;
    use rand::{self, seq::SliceRandom, RngExt, SeedableRng};

    #[test]
    fn test_id_allocator() {
        const START: u32 = 10;
        let allocator = ReloadableIDAllocator::new(START);
        let mut allocated_id = Vec::new();
        for i in 0..10000 {
            let id = allocator.allocate();
            assert_eq!(id, i + START);
            allocated_id.push(id);
        }
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xdeadbeef);
        allocated_id.shuffle(&mut rng);
        for _ in 0..5000 {
            let id = allocated_id.pop().unwrap();
            allocator.recycle(id);
        }
        let mut num = HashSet::new();
        for _ in 0..5000 {
            let id = allocator.allocate();
            num.insert(id);
            assert!(id <= START + 10000);
        }
        assert_eq!(num.len(), 5000);
    }

    #[test]
    fn test_id_allocator_reload() {
        const START: u32 = 1;
        let allocator = ReloadableIDAllocator::new(START);
        let mut rng = rand::rngs::SmallRng::seed_from_u64(0xdeadbeef);
        let mut occupied_ids = HashSet::new();
        while occupied_ids.len() < 200 {
            let id = rng.random_range(1..5000);
            occupied_ids.insert(id);
        }
        let occupied_ids = occupied_ids.into_iter().collect::<Vec<_>>();
        for id in occupied_ids.iter().copied() {
            allocator.occupy_idx(id);
        }

        let mut allocated_ids = HashSet::new();
        for _ in 0..5000 {
            let id = allocator.allocate();
            assert!(occupied_ids.iter().all(|val| *val != id));
            assert!(allocated_ids.insert(id));
            assert!(id <= 5200);
        }
        assert_eq!(allocated_ids.len(), 5000);
    }

    #[test]
    fn test_id_allocator_recycle_multiple() {
        const START: u32 = 1;
        let allocator = ReloadableIDAllocator::new(START);
        let mut occupied_ids = HashSet::new();
        for _ in 0..200 {
            let id = allocator.allocate();
            occupied_ids.insert(id);
        }
        assert_eq!(occupied_ids.len(), 200);

        while !occupied_ids.is_empty() {
            let id = occupied_ids.iter().next().copied().unwrap();
            occupied_ids.take(&id);
            // recycle the same id 5 times
            for _ in 0..5 {
                allocator.recycle(id);
            }
        }

        for _ in 0..1000 {
            // if the allocator does not support one id for multiple times
            // it might allocate same id multiple times
            let id = allocator.allocate();
            occupied_ids.insert(id);
        }
        assert_eq!(occupied_ids.len(), 1000);
    }

    #[test]
    fn test_id_allocator_is_free() {
        const START: u32 = 1;
        let allocator = ReloadableIDAllocator::new(START);
        allocator.occupy_idx(120);
        allocator.occupy_idx(33);
        allocator.occupy_idx(88);
        let mut occupied_ids: HashSet<u32> = HashSet::from_iter([120, 33, 88]);
        for _ in 0..200 {
            let id = allocator.allocate();
            occupied_ids.insert(id);
        }
        assert_eq!(occupied_ids.len(), 203);

        for id in 1..400 {
            let occupied = occupied_ids.contains(&id);
            assert_eq!(allocator.is_free(id), !occupied, "{id}");
        }
    }
}
