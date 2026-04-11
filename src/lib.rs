//! cuda-workflow: DAG-based workflow orchestration.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task { pub id: String, pub deps: Vec<String>, pub completed: bool, pub failed: bool }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStatus { pub total_tasks: usize, pub completed: usize, pub failed: usize, pub ready: Vec<String>, pub is_done: bool }

pub struct Workflow { tasks: HashMap<String, Task> }
impl Workflow {
    pub fn new() -> Self { Self { tasks: HashMap::new() } }
    pub fn add_task(&mut self, id: &str, deps: Vec<&str>) {
        self.tasks.insert(id.into(), Task { id: id.into(), deps: deps.iter().map(|s| s.to_string()).collect(), completed: false, failed: false });
    }
    pub fn ready_tasks(&self) -> Vec<String> {
        let completed: HashSet<String> = self.tasks.values().filter(|t| t.completed).map(|t| t.id.clone()).collect();
        self.tasks.values().filter(|t| !t.completed && !t.failed && t.deps.iter().all(|d| completed.contains(d))).map(|t| t.id.clone()).collect()
    }
    pub fn complete(&mut self, id: &str) { if let Some(t) = self.tasks.get_mut(id) { t.completed = true; } }
    pub fn fail(&mut self, id: &str) { if let Some(t) = self.tasks.get_mut(id) { t.failed = true; } }
    pub fn status(&self) -> WorkflowStatus {
        let total = self.tasks.len();
        let completed = self.tasks.values().filter(|t| t.completed).count();
        let failed = self.tasks.values().filter(|t| t.failed).count();
        let ready = self.ready_tasks();
        let done = completed + failed == total;
        WorkflowStatus { total_tasks: total, completed, failed, ready, is_done: done }
    }
    pub fn detect_cycle(&self) -> bool {
        let mut visited = HashSet::new(); let mut stack = HashSet::new();
        fn dfs(id: &str, tasks: &HashMap<String, Task>, visited: &mut HashSet<String>, stack: &mut HashSet<String>) -> bool {
            if stack.contains(id) { return true; }
            if visited.contains(id) { return false; }
            visited.insert(id.into()); stack.insert(id.into());
            if let Some(t) = tasks.get(id) { for d in &t.deps { if dfs(d, tasks, visited, stack) { return true; } } }
            stack.remove(id); false
        }
        for id in self.tasks.keys() { if dfs(id, &self.tasks, &mut visited, &mut stack) { return true; } }
        false
    }
    pub fn task_count(&self) -> usize { self.tasks.len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn test_empty_workflow() { let w = Workflow::new(); assert_eq!(w.task_count(), 0); assert!(w.status().is_done); }
    #[test] fn test_single_task() { let mut w = Workflow::new(); w.add_task("a", vec![]); assert_eq!(w.ready_tasks(), vec!["a"]); }
    #[test] fn test_dependency_chain() { let mut w = Workflow::new(); w.add_task("a", vec![]); w.add_task("b", vec!["a"]); assert!(w.ready_tasks().contains(&"a".into())); assert!(!w.ready_tasks().contains(&"b".into())); w.complete("a"); assert!(w.ready_tasks().contains(&"b".into())); }
    #[test] fn test_parallel_tasks() { let mut w = Workflow::new(); w.add_task("a", vec![]); w.add_task("b", vec![]); assert_eq!(w.ready_tasks().len(), 2); }
    #[test] fn test_cycle_detection() { let mut w = Workflow::new(); w.add_task("a", vec!["b"]); w.add_task("b", vec!["a"]); assert!(w.detect_cycle()); }
}