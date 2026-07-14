package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// Task represents a unit of work for the orchestrator, like deploying a service.
// It includes fields hinting at Docker container properties.
type Task struct {
	ID       string
	Name     string
	Image    string // e.g., "nginx:latest", simulating a Docker image
	Replicas int    // Number of instances to run
}

// Worker simulates a node (e.g., a VM in a cluster) that can execute tasks.
type Worker struct {
	ID        string
	TaskQueue chan Task // Channel to receive tasks from the orchestrator
	mu        sync.Mutex // Mutex for worker state management
	active    bool
}

// NewWorker creates and initializes a new Worker instance.
func NewWorker(id string) *Worker {
	return &Worker{
		ID:        id,
		TaskQueue: make(chan Task),
		active:    true,
	}
}

// Start begins the worker's task processing loop. It runs as a goroutine.
func (w *Worker) Start(wg *sync.WaitGroup) {
	defer wg.Done()
	fmt.Printf("Worker %s started, ready to receive tasks.\n", w.ID)
	for task := range w.TaskQueue { // Worker continuously listens for tasks
		if !w.active {
			fmt.Printf("Worker %s is shutting down, dropping task %s.\n", w.ID, task.ID)
			break
		}
		// Simulate the work of deploying a container based on the task.
		fmt.Printf("Worker %s received task %s: Deploying %s (replicas: %d)\n", w.ID, task.ID, task.Image, task.Replicas)
		time.Sleep(time.Duration(task.Replicas) * 500 * time.Millisecond) // Simulate deployment time
		fmt.Printf("Worker %s completed task %s.\n", w.ID, task.ID)
	}
	fmt.Printf("Worker %s stopped.\n", w.ID)
}

// Stop marks the worker as inactive and closes its task queue, signaling it to shut down.
func (w *Worker) Stop() {
	w.mu.Lock()
	w.active = false
	w.mu.Unlock()
	close(w.TaskQueue)
}

// Orchestrator manages tasks and distributes them to available workers.
// This simulates the core logic of a hybrid orchestrator.
type Orchestrator struct {
	workers    map[string]*Worker // Registered workers
	taskQueue  chan Task          // Incoming tasks awaiting assignment
	workerPool chan *Worker       // Pool of currently available workers
	mu         sync.Mutex
	wg         sync.WaitGroup     // Used to wait for all workers to finish
}

// NewOrchestrator creates and initializes a new Orchestrator instance.
func NewOrchestrator() *Orchestrator {
	return &Orchestrator{
		workers:    make(map[string]*Worker),
		taskQueue:  make(chan Task, 10), // Buffered channel for incoming tasks
		workerPool: make(chan *Worker, 5), // Buffered channel for available workers
	}
}

// AddWorker registers a new worker with the orchestrator and starts its processing loop.
func (o *Orchestrator) AddWorker(worker *Worker) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.workers[worker.ID] = worker
	o.wg.Add(1)
	go worker.Start(&o.wg) // Start the worker's processing goroutine
	o.workerPool <- worker // Add worker to the available pool
	fmt.Printf("Orchestrator added Worker %s to the cluster.\n", worker.ID)
}

// SubmitTask adds a new task to the orchestrator's queue for processing.
func (o *Orchestrator) SubmitTask(task Task) {
	o.taskQueue <- task
	fmt.Printf("Orchestrator submitted task %s: %s (Image: %s)\n", task.ID, task.Name, task.Image)
}

// StartOrchestratorLoop begins the continuous process of distributing tasks to workers.
func (o *Orchestrator) StartOrchestratorLoop() {
	go func() {
		for task := range o.taskQueue { // Orchestrator listens for new tasks
			// This simulates finding an available worker from the multi-node cluster.
			worker := <-o.workerPool // Blocks until a worker is available
			fmt.Printf("Orchestrator assigning task %s to Worker %s.\n", task.ID, worker.ID)
			worker.TaskQueue <- task // Send task to the chosen worker
			// In a real system, the worker would report completion, and then be re-added.
			// For this simulation, we immediately return the worker to the pool.
			o.workerPool <- worker
		}
	}()
}

// StopOrchestrator gracefully shuts down the orchestrator and all its workers.
func (o *Orchestrator) StopOrchestrator() {
	fmt.Println("\nOrchestrator shutting down...")
	close(o.taskQueue) // Stop accepting new tasks
	// Give some time for pending tasks to be processed by workers
	time.Sleep(1 * time.Second)

	for _, worker := range o.workers {
		worker.Stop() // Signal each worker to stop
	}
	o.wg.Wait() // Wait for all worker goroutines to finish
	fmt.Println("Orchestrator and all workers stopped.")
}

func main() {
	fmt.Println("--- Go Hybrid Docker Orchestrator Simulation ---")

	orchestrator := NewOrchestrator()

	// Add multiple workers, simulating a multi-node cluster environment.
	// Each worker runs as a separate goroutine.
	orchestrator.AddWorker(NewWorker("worker-01"))
	orchestrator.AddWorker(NewWorker("worker-02"))
	orchestrator.AddWorker(NewWorker("worker-03"))

	// Start the orchestrator's main loop to distribute tasks.
	orchestrator.StartOrchestratorLoop()

	// Submit various tasks, simulating different Docker container deployments.
	log.Println("Submitting tasks...")
	orchestrator.SubmitTask(Task{ID: "task-001", Name: "Web Service A", Image: "my-app:v1.0", Replicas: 2})
	orchestrator.SubmitTask(Task{ID: "task-002", Name: "Database", Image: "postgres:14", Replicas: 1})
	orchestrator.SubmitTask(Task{ID: "task-003", Name: "Cache Service", Image: "redis:6", Replicas: 3})
	orchestrator.SubmitTask(Task{ID: "task-004", Name: "API Gateway", Image: "traefik:v2.8", Replicas: 2})
	orchestrator.SubmitTask(Task{ID: "task-005", Name: "Monitoring Agent", Image: "prometheus/node-exporter", Replicas: 1})

	// Give some time for tasks to be processed by the simulated workers.
	time.Sleep(6 * time.Second)

	// Initiate graceful shutdown of the orchestrator and its workers.
	orchestrator.StopOrchestrator()

	fmt.Println("--- Simulation End ---")
}
