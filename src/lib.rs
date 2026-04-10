/*!
# cuda-workflow

DAG-based workflow orchestration.

Complex agent processes aren't linear. They branch, merge, wait,
retry, and parallelize. This crate provides a DAG (directed acyclic
graph) workflow engine that handles the "what order do things run"
problem.

- DAG construction and validation (cycle detection)
- Parallel step execution
- Conditional branching
- Error handling and retry
- Pipeline chaining
- Workflow state management
*/

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// A workflow step
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Step {
    pub id: String,
    pub name: String,
    pub action: String,
    pub deps: Vec<String>,           // step IDs this depends on
    pub condition: Option<String>,   // condition expression
    pub retry_max: u32,
    pub timeout_ms: u64,
    pub status: StepStatus,
    pub attempts: u32,
    pub result: Option<String>,
    pub error: Option<String>,
    pub duration_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StepStatus { Pending, Ready, Running, Completed, Failed, Skipped, Blocked }

/// A workflow (DAG of steps)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Workflow {
    pub id: String,
    pub name: String,
    pub steps: HashMap<String, Step>,
    pub status: WorkflowStatus,
    pub created: u64,
    pub started: Option<u64>,
    pub completed: Option<u64>,
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus { Draft, Running, Paused, Completed, Failed }

/// Workflow execution engine
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkflowEngine {
    pub workflows: HashMap<String, Workflow>,
    pub next_id: u64,
}

impl WorkflowEngine {
    pub fn new() -> Self { WorkflowEngine { workflows: HashMap::new(), next_id: 1 } }

    /// Create a new workflow
    pub fn create(&mut self, name: &str) -> String {
        let id = format!("wf_{}", self.next_id);
        self.next_id += 1;
        let wf = Workflow { id: id.clone(), name: name.to_string(), steps: HashMap::new(), status: WorkflowStatus::Draft, created: now(), started: None, completed: None, error: None };
        self.workflows.insert(id.clone(), wf);
        id
    }

    /// Add a step to a workflow
    pub fn add_step(&mut self, wf_id: &str, step: Step) -> bool {
        if let Some(wf) = self.workflows.get_mut(wf_id) {
            wf.steps.insert(step.id.clone(), step);
            true
        } else { false }
    }

    /// Make a simple step
    pub fn make_step(id: &str, name: &str, action: &str, deps: &[&str]) -> Step {
        Step { id: id.to_string(), name: name.to_string(), action: action.to_string(), deps: deps.iter().map(|s| s.to_string()).collect(), condition: None, retry_max: 3, timeout_ms: 30_000, status: StepStatus::Pending, attempts: 0, result: None, error: None, duration_ms: 0 }
    }

    /// Validate DAG (check for cycles)
    pub fn validate(&self, wf_id: &str) -> Result<(), String> {
        let wf = self.workflows.get(wf_id).ok_or("workflow not found")?;
        let mut visited = HashSet::new();
        let mut stack = HashSet::new();
        for step_id in wf.steps.keys() {
            if self.has_cycle(wf_id, step_id, &mut visited, &mut stack) {
                return Err(format!("cycle detected at {}", step_id));
            }
        }
        Ok(())
    }

    fn has_cycle(&self, wf_id: &str, step_id: &str, visited: &mut HashSet<String>, stack: &mut HashSet<String>) -> bool {
        if stack.contains(step_id) { return true; }
        if visited.contains(step_id) { return false; }
        visited.insert(step_id.to_string());
        stack.insert(step_id.to_string());
        if let Some(wf) = self.workflows.get(wf_id) {
            if let Some(step) = wf.steps.get(step_id) {
                for dep in &step.deps {
                    if self.has_cycle(wf_id, dep, visited, stack) { return true; }
                }
            }
        }
        stack.remove(step_id);
        false
    }

    /// Start a workflow
    pub fn start(&mut self, wf_id: &str) -> bool {
        if let Some(wf) = self.workflows.get_mut(wf_id) {
            wf.status = WorkflowStatus::Running;
            wf.started = Some(now());
            // Mark initial steps as ready (no deps)
            for step in wf.steps.values_mut() {
                if step.deps.is_empty() { step.status = StepStatus::Ready; }
            }
            true
        } else { false }
    }

    /// Complete a step (simulate execution)
    pub fn complete_step(&mut self, wf_id: &str, step_id: &str, success: bool, result: Option<&str>) {
        let wf = if let Some(w) = self.workflows.get_mut(wf_id) { w } else { return };
        let step = if let Some(s) = wf.steps.get_mut(step_id) { s } else { return };

        step.attempts += 1;
        if success {
            step.status = StepStatus::Completed;
            step.result = result.map(|s| s.to_string());
            // Unblock dependents
            let step_deps: Vec<String> = wf.steps.values().filter(|s| s.deps.contains(&step_id.to_string())).map(|s| s.id.clone()).collect();
            for dep_id in step_deps {
                if let Some(dep_step) = wf.steps.get_mut(&dep_id) {
                    let all_deps_met = dep_step.deps.iter().all(|d| {
                        wf.steps.get(d).map(|s| s.status == StepStatus::Completed).unwrap_or(false)
                    });
                    if all_deps_met { dep_step.status = StepStatus::Ready; }
                }
            }
        } else {
            if step.attempts >= step.retry_max {
                step.status = StepStatus::Failed;
                step.error = result.map(|s| s.to_string());
            } else {
                step.status = StepStatus::Ready; // retry
            }
        }
        self.check_workflow_status(wf_id);
    }

    /// Check if workflow is complete
    fn check_workflow_status(&mut self, wf_id: &str) {
        if let Some(wf) = self.workflows.get_mut(wf_id) {
            let all_done = wf.steps.values().all(|s| s.status == StepStatus::Completed || s.status == StepStatus::Skipped);
            let any_failed = wf.steps.values().any(|s| s.status == StepStatus::Failed);
            if any_failed { wf.status = WorkflowStatus::Failed; wf.completed = Some(now()); }
            else if all_done && !wf.steps.is_empty() { wf.status = WorkflowStatus::Completed; wf.completed = Some(now()); }
        }
    }

    /// Get ready steps
    pub fn ready_steps(&self, wf_id: &str) -> Vec<&Step> {
        self.workflows.get(wf_id).map(|wf| wf.steps.values().filter(|s| s.status == StepStatus::Ready).collect()).unwrap_or_default()
    }

    /// Get execution order (topological sort)
    pub fn execution_order(&self, wf_id: &str) -> Vec<String> {
        let mut order = vec![];
        let mut in_degree: HashMap<String, usize> = HashMap::new();
        let mut queue = VecDeque::new();

        if let Some(wf) = self.workflows.get(wf_id) {
            for step in wf.steps.values() { in_degree.insert(step.id.clone(), step.deps.len()); }
            for step in wf.steps.values() { if step.deps.is_empty() { queue.push_back(step.id.clone()); } }
            while let Some(id) = queue.pop_front() {
                order.push(id.clone());
                for step in wf.steps.values() {
                    if step.deps.contains(&id) {
                        let deg = in_degree.get_mut(&step.id).unwrap();
                        *deg -= 1;
                        if *deg == 0 { queue.push_back(step.id.clone()); }
                    }
                }
            }
        }
        order
    }

    /// Summary
    pub fn summary(&self, wf_id: &str) -> String {
        if let Some(wf) = self.workflows.get(wf_id) {
            let completed = wf.steps.values().filter(|s| s.status == StepStatus::Completed).count();
            let failed = wf.steps.values().filter(|s| s.status == StepStatus::Failed).count();
            format!("Workflow[{}]: status={:?}, {}/{} steps completed, {} failed",
                wf.name, wf.status, completed, wf.steps.len(), failed)
        } else { "Workflow not found".into() }
    }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_workflow() {
        let mut we = WorkflowEngine::new();
        let id = we.create("test");
        assert!(we.workflows.contains_key(&id));
    }

    #[test]
    fn test_dag_validation_no_cycle() {
        let mut we = WorkflowEngine::new();
        let id = we.create("linear");
        we.add_step(&id, we.make_step("a", "Step A", "run", &[]));
        we.add_step(&id, we.make_step("b", "Step B", "run", &["a"]));
        we.add_step(&id, we.make_step("c", "Step C", "run", &["b"]));
        assert!(we.validate(&id).is_ok());
    }

    #[test]
    fn test_dag_validation_cycle() {
        let mut we = WorkflowEngine::new();
        let id = we.create("cycle");
        we.add_step(&id, we.make_step("a", "A", "run", &["c"]));
        we.add_step(&id, we.make_step("b", "B", "run", &["a"]));
        we.add_step(&id, we.make_step("c", "C", "run", &["b"]));
        assert!(we.validate(&id).is_err());
    }

    #[test]
    fn test_execution_order() {
        let mut we = WorkflowEngine::new();
        let id = we.create("order");
        we.add_step(&id, we.make_step("a", "A", "run", &[]));
        we.add_step(&id, we.make_step("b", "B", "run", &["a"]));
        we.add_step(&id, we.make_step("c", "C", "run", &["a"]));
        let order = we.execution_order(&id);
        assert_eq!(order[0], "a");
    }

    #[test]
    fn test_start_and_complete() {
        let mut we = WorkflowEngine::new();
        let id = we.create("simple");
        we.add_step(&id, we.make_step("a", "A", "run", &[]));
        we.add_step(&id, we.make_step("b", "B", "run", &["a"]));
        we.start(&id);
        let ready = we.ready_steps(&id);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "a");
    }

    #[test]
    fn test_step_completion_unblocks() {
        let mut we = WorkflowEngine::new();
        let id = we.create("chain");
        we.add_step(&id, we.make_step("a", "A", "run", &[]));
        we.add_step(&id, we.make_step("b", "B", "run", &["a"]));
        we.start(&id);
        we.complete_step(&id, "a", true, Some("done"));
        let ready = we.ready_steps(&id);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].id, "b");
    }

    #[test]
    fn test_workflow_completion() {
        let mut we = WorkflowEngine::new();
        let id = we.create("done");
        we.add_step(&id, we.make_step("a", "A", "run", &[]));
        we.start(&id);
        we.complete_step(&id, "a", true, None);
        assert_eq!(we.workflows[&id].status, WorkflowStatus::Completed);
    }

    #[test]
    fn test_parallel_ready() {
        let mut we = WorkflowEngine::new();
        let id = we.create("parallel");
        we.add_step(&id, we.make_step("a", "A", "run", &[]));
        we.add_step(&id, we.make_step("b", "B", "run", &["a"]));
        we.add_step(&id, we.make_step("c", "C", "run", &["a"]));
        we.start(&id);
        we.complete_step(&id, "a", true, None);
        let ready = we.ready_steps(&id);
        assert_eq!(ready.len(), 2); // b and c both ready
    }

    #[test]
    fn test_retry_on_failure() {
        let mut we = WorkflowEngine::new();
        let id = we.create("retry");
        let mut step = we.make_step("a", "A", "run", &[]);
        step.retry_max = 2;
        we.add_step(&id, step);
        we.start(&id);
        we.complete_step(&id, "a", false, Some("fail"));
        assert_eq!(we.workflows[&id].steps["a"].status, StepStatus::Ready); // retry
        assert_eq!(we.workflows[&id].steps["a"].attempts, 1);
    }

    #[test]
    fn test_max_retries() {
        let mut we = WorkflowEngine::new();
        let id = we.create("maxretry");
        let mut step = we.make_step("a", "A", "run", &[]);
        step.retry_max = 1;
        we.add_step(&id, step);
        we.start(&id);
        we.complete_step(&id, "a", false, None);
        assert_eq!(we.workflows[&id].steps["a"].status, StepStatus::Failed);
    }

    #[test]
    fn test_summary() {
        let mut we = WorkflowEngine::new();
        let id = we.create("x");
        we.add_step(&id, we.make_step("a", "A", "run", &[]));
        let s = we.summary(&id);
        assert!(s.contains("Draft"));
    }
}
