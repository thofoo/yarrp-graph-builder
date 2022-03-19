use std::collections::VecDeque;

pub struct Stack<T> {
    stack: Vec<T>,
}

impl <T> Stack<T> {
    pub fn new() -> Stack<T> {
        Stack { stack: Vec::new() }
    }

    pub fn pop(&mut self) -> Option<T> {
        self.stack.pop()
    }

    // "unsafe" pop, pop with unwrap
    pub fn upop(&mut self) -> T {
        self.pop().unwrap()
    }

    pub fn push(&mut self, item: T) {
        self.stack.push(item)
    }

    pub fn is_empty(&self) -> bool {
        self.stack.is_empty()
    }
}

pub struct Queue<T> {
    queue: VecDeque<T>,
}

impl <T> Queue<T> {
    pub fn new() -> Queue<T> {
        Queue { queue: VecDeque::new() }
    }

    pub fn poll(&mut self) -> Option<T> {
        self.queue.pop_front()
    }

    // "unsafe" poll, poll with unwrap
    pub fn upoll(&mut self) -> T {
        self.poll().unwrap()
    }

    pub fn push(&mut self, item: T) {
        self.queue.push_back(item)
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
}