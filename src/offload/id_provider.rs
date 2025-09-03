use std::sync::Mutex;

use uuid::Uuid;

pub trait IdProvider {
    fn generate(&self) -> String;
}

#[derive(Debug)]
pub struct FixedIdsProvider {
    pub ids: Mutex<Vec<String>>,
}

impl FixedIdsProvider {
    pub fn new(ids: Vec<&str>) -> Self {
        Self {
            ids: Mutex::new(ids.into_iter().map(|i| i.to_owned()).collect()),
        }
    }
}

impl IdProvider for FixedIdsProvider {
    fn generate(&self) -> String {
        let mut locked = self.ids.lock().expect("lock ids");

        if !locked.is_empty() {
            // Act as an infinite circular buffer
            let taken = locked.remove(0);
            locked.push(taken.clone());
            taken
        } else {
            panic!("FixedIdsProvider was exhausted")
        }
    }
}

#[derive(Debug, Default)]
pub struct RandomUuidProvider {}

impl IdProvider for RandomUuidProvider {
    fn generate(&self) -> String {
        Uuid::new_v4().to_string()
    }
}
