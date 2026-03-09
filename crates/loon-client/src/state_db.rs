pub trait StateDb {
    fn checkpoint(&mut self, label: &str);
}
