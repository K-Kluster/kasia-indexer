use std::borrow::Borrow;
use std::cmp::Ordering;

/// Two-pointer intersection iterator that finds elements from the first iterator
/// that have keys present in the second iterator
pub struct TwoPointerIntersection<L: Iterator, R: Iterator, K, V, E> {
    left: std::iter::Peekable<L>,
    right: std::iter::Peekable<R>,
    _phantom: std::marker::PhantomData<(K, V, E)>,
}

impl<U, T, K, V, E, Q> TwoPointerIntersection<U, T, K, V, E>
where
    U: Iterator<Item = Result<(K, V), E>>,
    T: Iterator<Item = Q>,
    K: Ord,
    Q: Borrow<K>,
{
    pub fn new(left: U, right: T) -> Self {
        Self {
            left: left.peekable(),
            right: right.peekable(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<U, T, K, V, E, Q> Iterator for TwoPointerIntersection<U, T, K, V, E>
where
    U: Iterator<Item = Result<(K, V), E>>,
    T: Iterator<Item = Q>,
    K: Ord,
    Q: Borrow<K>,
{
    type Item = Result<(K, V), E>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Peek at both iterators
            let left_peek = self.left.peek()?;
            let right_peek = self.right.peek()?;

            // Handle error in left iterator
            let (left_key, _) = match left_peek {
                Ok((key, value)) => (key, value),
                Err(_) => {
                    // Return the error and advance left iterator
                    return self.left.next();
                }
            };

            match left_key.cmp(right_peek.borrow()) {
                Ordering::Equal => {
                    // Found intersection - advance both and return the left item
                    let result = self.left.next();
                    self.right.next(); // advance right iterator
                    return result;
                }
                Ordering::Less => {
                    // left_key < right_key, advance left iterator
                    self.left.next();
                }
                Ordering::Greater => {
                    // left_key > right_key, advance right iterator
                    self.right.next();
                }
            }
        }
    }
}

/// Trait for creating two-pointer intersections
pub trait TwoPointerIntersect<T, K, V, E, Q>: Iterator<Item = Result<(K, V), E>> + Sized
where
    T: Iterator<Item = Q>,
    K: Ord,
    Q: Borrow<K>,
{
    fn intersect_with(self, other: T) -> TwoPointerIntersection<Self, T, K, V, E> {
        TwoPointerIntersection::new(self, other)
    }
}

// Blanket implementation for all iterators that match our pattern
impl<I, T, K, V, E, Q> TwoPointerIntersect<T, K, V, E, Q> for I
where
    I: Iterator<Item = Result<(K, V), E>>,
    T: Iterator<Item = Q>,
    K: Ord,
    Q: Borrow<K>,
{
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct TestKey(u32);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestValue(String);

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct TestError(String);

    #[test]
    fn test_two_pointer_intersection_basic() {
        // Left iterator: sorted key-value pairs with some errors
        let left_data = vec![
            Ok::<_, Infallible>((TestKey(1), TestValue("a".to_string()))),
            Ok((TestKey(3), TestValue("b".to_string()))),
            Ok((TestKey(5), TestValue("c".to_string()))),
            Ok((TestKey(7), TestValue("d".to_string()))),
            Ok((TestKey(9), TestValue("e".to_string()))),
        ];

        // Right iterator: sorted keys to find
        let right_data = vec![TestKey(2), TestKey(3), TestKey(5), TestKey(8), TestKey(9)];

        let result: Vec<_> =
            TwoPointerIntersection::new(left_data.into_iter(), right_data.into_iter()).collect();

        let expected = vec![
            Ok((TestKey(3), TestValue("b".to_string()))),
            Ok((TestKey(5), TestValue("c".to_string()))),
            Ok((TestKey(9), TestValue("e".to_string()))),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_two_pointer_intersection_with_errors() {
        let left_data = vec![
            Ok((TestKey(1), TestValue("a".to_string()))),
            Err(TestError("error at key 2".to_string())),
            Ok((TestKey(3), TestValue("b".to_string()))),
            Ok((TestKey(5), TestValue("c".to_string()))),
        ];

        let right_data = vec![TestKey(1), TestKey(3), TestKey(4)];

        let result: Vec<_> =
            TwoPointerIntersection::new(left_data.into_iter(), right_data.into_iter()).collect();

        let expected = vec![
            Ok((TestKey(1), TestValue("a".to_string()))),
            Err(TestError("error at key 2".to_string())),
            Ok((TestKey(3), TestValue("b".to_string()))),
        ];

        assert_eq!(result, expected);
    }

    #[test]
    fn test_two_pointer_intersection_no_matches() {
        let left_data = vec![
            Ok::<_, Infallible>((TestKey(1), TestValue("a".to_string()))),
            Ok((TestKey(3), TestValue("b".to_string()))),
            Ok((TestKey(5), TestValue("c".to_string()))),
        ];

        let right_data = vec![TestKey(2), TestKey(4), TestKey(6)];

        let result: Vec<_> =
            TwoPointerIntersection::new(left_data.into_iter(), right_data.into_iter()).collect();

        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_two_pointer_intersection_empty_iterators() {
        let left_data: Vec<Result<(TestKey, TestValue), TestError>> = vec![];
        let right_data: Vec<TestKey> = vec![];

        let result: Vec<_> =
            TwoPointerIntersection::new(left_data.into_iter(), right_data.into_iter()).collect();

        assert_eq!(result, vec![]);
    }

    #[test]
    fn test_two_pointer_intersection_trait() {
        let left_data = vec![
            Ok::<_, Infallible>((TestKey(1), TestValue("a".to_string()))),
            Ok((TestKey(3), TestValue("b".to_string()))),
            Ok((TestKey(5), TestValue("c".to_string()))),
        ];

        let right_data = vec![TestKey(1), TestKey(5)];

        let result: Vec<_> = left_data
            .into_iter()
            .intersect_with(right_data.into_iter())
            .collect();

        let expected = vec![
            Ok((TestKey(1), TestValue("a".to_string()))),
            Ok((TestKey(5), TestValue("c".to_string()))),
        ];

        assert_eq!(result, expected);
    }
}

// Example usage for your specific case:
/*
pub fn check_unknown_batch<'a, I>(
    &self,
    wtx: &mut WriteTransaction,
    tx_ids: I,
) -> impl Iterator<Item = Result<(RpcTransactionId, RpcHash), Error>>
where
    I: Iterator<Item = &'a RpcTransactionId>, // lexicographically ordered
{
    let unknowns = wtx.iter(&self.0).map(|r| {
        r.map_err(anyhow::Error::from).and_then(|(k: UserKey, v: UserValue)| {
            if k.len() != 32 || v.len() != 32 {
                bail!("Invalid key/value length in unknown_tx partition");
            }
            let tx_id: RpcTransactionId = RpcTransactionId::from_slice(&k)?;
            let accepting_block_hash: RpcHash = RpcHash::from_slice(&v)?;
            Ok((tx_id, accepting_block_hash))
        })
    });

    TwoPointerIntersection::new(unknowns, tx_ids.cloned())
    // Or using the trait:
    // unknowns.intersect_with(tx_ids.cloned())
}
*/
