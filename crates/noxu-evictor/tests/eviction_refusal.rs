//! A refused detach is not an eviction and must stay eligible for retry.
use noxu_evictor::{Arbiter, CacheMode, EvictionSource, Evictor};
use noxu_tree::tree::Tree;
use noxu_util::NULL_LSN;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicI64, Ordering},
};

#[test]
fn refused_detach_does_not_credit_eviction() {
    let tree = Tree::new(1, 8);
    for i in 0..20u8 {
        tree.insert(vec![i], vec![i], NULL_LSN).unwrap();
    }
    let id = tree.search_with_data(&[0]).unwrap().bin_arc.read().node_id();
    let tree = Arc::new(RwLock::new(tree));
    let usage = Arc::new(AtomicI64::new(100_000));
    let evictor =
        Evictor::new(Arbiter::new(1, Arc::clone(&usage), 1, 0), 1, true)
            .with_tree(Arc::clone(&tree), 1)
            .with_mutate_bins(false);
    evictor.note_ins_added(id, CacheMode::Default);
    // No WAL image: the shared detach guard refuses. Exercise that same
    // refusal result as a moved parent slot, without a timing-based race.
    for _ in 0..32 {
        let result = evictor.do_evict(EvictionSource::Manual);
        assert_eq!(result.nodes_evicted, 0);
        assert_eq!(result.bytes_evicted, 0);
        assert_eq!(usage.load(Ordering::Relaxed), 100_000);
        assert!(tree.read().unwrap().get_parent_in_for_child_in(id).is_some());
        assert_eq!(evictor.get_policy_sizes(), (1, 0, 0));
        assert_eq!(
            evictor.get_stats().get(&evictor.get_stats().nodes_evicted),
            0
        );
        // Removing/reinserting also exercises the list's ID index, not only
        // its length. Repeated insertion must not create duplicate links.
        evictor.note_ins_removed(id);
        assert_eq!(evictor.get_policy_sizes(), (0, 0, 0));
        evictor.note_ins_added(id, CacheMode::Default);
        evictor.note_ins_added(id, CacheMode::Default);
        assert_eq!(evictor.get_policy_sizes(), (1, 0, 0));
    }
}
