use ringmap::set::RingSet;

pub struct FifoSet<K> {
    set: RingSet<K>,
}

impl<K: std::hash::Hash + Eq> FifoSet<K> {
    pub fn new(capacity: usize) -> Self {
        Self {
            set: RingSet::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, key: K) -> bool {
        if self.set.contains(&key) {
            return false;
        }

        if self.set.len() == self.set.capacity() {
            self.set.pop_front();
        }

        self.set.insert(key);
        true
    }

    pub fn contains(&self, key: &K) -> bool {
        self.set.contains(key)
    }
}
