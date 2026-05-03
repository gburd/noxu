//! Replication output thread abstraction.
//!
//! Provides a bounded, thread-safe queue for outbound replication messages.
//! The `OutputQueue` decouples message production (by the feeder or
//! replication logic) from message consumption (by the network I/O thread).

use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Manages a queue of outbound replication messages.
///
/// The queue has a configurable maximum size. When the queue is full or
/// shut down, `enqueue` returns `false`. Messages can be dequeued in
/// batches for efficient network I/O.
pub struct OutputQueue {
    /// The message queue.
    queue: Mutex<Vec<Vec<u8>>>,
    /// Whether the queue has been shut down.
    shutdown: AtomicBool,
    /// Maximum number of messages the queue can hold.
    max_queue_size: usize,
}

impl OutputQueue {
    /// Create a new output queue with the given maximum capacity.
    pub fn new(max_queue_size: usize) -> Self {
        OutputQueue {
            queue: Mutex::new(Vec::new()),
            shutdown: AtomicBool::new(false),
            max_queue_size,
        }
    }

    /// Enqueue a message.
    ///
    /// Returns `true` if the message was accepted, `false` if the queue
    /// is full or has been shut down.
    pub fn enqueue(&self, message: Vec<u8>) -> bool {
        if self.shutdown.load(Ordering::Acquire) {
            return false;
        }
        let mut queue = self.queue.lock();
        if queue.len() >= self.max_queue_size {
            return false;
        }
        queue.push(message);
        true
    }

    /// Dequeue up to `max` messages in a batch.
    ///
    /// Returns the messages in FIFO order. If fewer than `max` messages
    /// are available, returns all of them.
    pub fn dequeue_batch(&self, max: usize) -> Vec<Vec<u8>> {
        let mut queue = self.queue.lock();
        let count = max.min(queue.len());
        queue.drain(..count).collect()
    }

    /// Return the number of messages currently in the queue.
    pub fn len(&self) -> usize {
        self.queue.lock().len()
    }

    /// Return true if the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.lock().is_empty()
    }

    /// Shut down the queue. No more messages will be accepted.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Return true if the queue has been shut down.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// Clear all messages from the queue.
    pub fn clear(&self) {
        self.queue.lock().clear();
    }
}

impl std::fmt::Debug for OutputQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OutputQueue")
            .field("len", &self.len())
            .field("max_queue_size", &self.max_queue_size)
            .field("shutdown", &self.is_shutdown())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_queue() {
        let q = OutputQueue::new(10);
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        assert!(!q.is_shutdown());
    }

    #[test]
    fn test_enqueue_dequeue() {
        let q = OutputQueue::new(10);
        assert!(q.enqueue(vec![1, 2, 3]));
        assert!(q.enqueue(vec![4, 5]));
        assert_eq!(q.len(), 2);
        assert!(!q.is_empty());

        let batch = q.dequeue_batch(10);
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0], vec![1, 2, 3]);
        assert_eq!(batch[1], vec![4, 5]);
        assert!(q.is_empty());
    }

    #[test]
    fn test_batch_dequeue_partial() {
        let q = OutputQueue::new(100);
        for i in 0..10 {
            q.enqueue(vec![i]);
        }
        assert_eq!(q.len(), 10);

        let batch = q.dequeue_batch(3);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0], vec![0]);
        assert_eq!(batch[1], vec![1]);
        assert_eq!(batch[2], vec![2]);
        assert_eq!(q.len(), 7);

        let batch2 = q.dequeue_batch(5);
        assert_eq!(batch2.len(), 5);
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn test_capacity_limit() {
        let q = OutputQueue::new(3);
        assert!(q.enqueue(vec![1]));
        assert!(q.enqueue(vec![2]));
        assert!(q.enqueue(vec![3]));
        // Queue is full.
        assert!(!q.enqueue(vec![4]));
        assert_eq!(q.len(), 3);

        // After draining one, we can enqueue again.
        q.dequeue_batch(1);
        assert!(q.enqueue(vec![4]));
        assert_eq!(q.len(), 3);
    }

    #[test]
    fn test_shutdown_rejects_enqueue() {
        let q = OutputQueue::new(10);
        assert!(q.enqueue(vec![1]));
        q.shutdown();
        assert!(q.is_shutdown());
        assert!(!q.enqueue(vec![2]));
        // Can still drain existing messages.
        let batch = q.dequeue_batch(10);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_clear() {
        let q = OutputQueue::new(10);
        q.enqueue(vec![1]);
        q.enqueue(vec![2]);
        q.enqueue(vec![3]);
        assert_eq!(q.len(), 3);

        q.clear();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
    }

    #[test]
    fn test_dequeue_empty() {
        let q = OutputQueue::new(10);
        let batch = q.dequeue_batch(5);
        assert!(batch.is_empty());
    }

    #[test]
    fn test_dequeue_batch_zero() {
        let q = OutputQueue::new(10);
        q.enqueue(vec![1]);
        let batch = q.dequeue_batch(0);
        assert!(batch.is_empty());
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn test_shutdown_then_clear() {
        let q = OutputQueue::new(10);
        q.enqueue(vec![1]);
        q.enqueue(vec![2]);
        q.shutdown();
        q.clear();
        assert!(q.is_empty());
        assert!(q.is_shutdown());
    }

    #[test]
    fn test_concurrent_enqueue() {
        use std::sync::Arc;
        use std::thread;

        let q = Arc::new(OutputQueue::new(1000));
        let mut handles = vec![];

        for t in 0..4 {
            let queue = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                let mut count = 0;
                for i in 0..100 {
                    if queue.enqueue(vec![t, i]) {
                        count += 1;
                    }
                }
                count
            }));
        }

        let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(q.len(), total);
        assert!(total <= 400);
    }

    #[test]
    fn test_debug_format() {
        let q = OutputQueue::new(42);
        q.enqueue(vec![1]);
        let debug = format!("{:?}", q);
        assert!(debug.contains("OutputQueue"));
        assert!(debug.contains("max_queue_size: 42"));
    }
}
