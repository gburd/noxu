//! A range seek whose floor BIN holds no live slot >= the seek key must
//! advance to the next BIN instead of reporting "nothing at or after".
//!
//! JE `Cursor.searchRange` positions on the floor BIN and, when the key is
//! not there, calls `searchRangeAdvanceAndCheckKey` -> `CursorImpl.getNext`
//! (Cursor.java:4223-4300, :4523-4550).  A seek key in the gap between two
//! BINs (above the floor BIN's last key, below the next BIN's identifier
//! key) is routed to the floor BIN, so without the advance the seek misses.

use noxu_tree::{Tree, TreeNode};
use noxu_util::Lsn;

/// Even keys 0, 2, .., 78 at fanout 4, so every odd key is a gap key and
/// some gap keys fall between two BINs.
fn even_keys() -> Tree {
    let tree = Tree::new(1, 4);
    for i in 0u8..40 {
        tree.insert(vec![i * 2], vec![i], Lsn::from_u64(u64::from(i) + 1))
            .unwrap();
    }
    tree
}

fn check(tree: &Tree, probe: &[u8], want: Option<Vec<u8>>) {
    assert_eq!(
        tree.first_entry_at_or_after(probe).map(|r| r.0),
        want,
        "first_entry_at_or_after({probe:?})"
    );
    let r = tree.first_entry_at_or_after_with_index(probe);
    assert_eq!(
        r.as_ref().map(|r| r.0.clone()),
        want,
        "first_entry_at_or_after_with_index({probe:?})"
    );
    if let Some((k, _, idx, _, arc)) = r {
        let g = arc.read();
        let TreeNode::Bottom(bin) = &*g else {
            panic!("returned arc not a BIN")
        };
        assert_eq!(bin.get_full_key(idx), Some(k), "idx/arc disagree");
    }
}

#[test]
fn gap_key_between_bins_resolves_to_next_bin() {
    let tree = even_keys();
    for i in 0u8..39 {
        check(&tree, &[i * 2 + 1], Some(vec![i * 2 + 2]));
    }
    check(&tree, &[79], None);
}

#[test]
fn emptied_leading_bins_resolve_to_first_live_key() {
    let tree = even_keys();
    for i in 0u8..10 {
        assert!(tree.delete(&[i * 2]));
    }
    for probe in 0u8..=20 {
        check(&tree, &[probe], Some(vec![20]));
    }
}
