use indexmap::IndexSet;

pub struct FifoSet<K> {
    set: IndexSet<K>,
    capacity: usize,
}

impl<K: std::hash::Hash + Eq> FifoSet<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: IndexSet::with_capacity(capacity),
            capacity,
        }
    }

    pub fn insert(&mut self, key: K) -> bool {
        if self.set.contains(&key) {
            return false;
        }

        if self.set.len() == self.capacity {
            self.set.shift_remove_index(0);
        }

        self.set.insert(key);
        true
    }

    pub fn contains(&self, key: &K) -> bool {
        self.set.contains(key)
    }
}
