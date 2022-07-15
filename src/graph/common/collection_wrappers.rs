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
