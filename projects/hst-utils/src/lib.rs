use std::collections::HashSet;
use std::hash::Hash;

pub fn dedup_unsorted<T: Eq + Hash + Copy>(values: &mut Vec<T>) -> usize {
    let mut seen = HashSet::new();
    let mut removed_count = 0;

    values.retain(|value| {
        if seen.contains(value) {
            removed_count += 1;
            false
        } else {
            seen.insert(*value);
            true
        }
    });

    removed_count
}

#[cfg(test)]
mod tests {
    #[test]
    fn dedup_unsorted() {
        let mut input = vec![1, 2, 3, 4, 1, 100, 2];
        super::dedup_unsorted(&mut input);
        assert_eq!(input, vec![1, 2, 3, 4, 100]);
    }
}
