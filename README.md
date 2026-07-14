# Go Hybrid Orchestrator Simulation

This Go program simulates a basic hybrid Docker orchestrator. It demonstrates how an orchestrator can define tasks (like deploying container images), manage a pool of workers (simulating multiple nodes), and distribute tasks to them concurrently using Go's goroutines and channels. It provides a foundational understanding of task scheduling and worker management in a distributed system context, abstracting away the actual Docker interactions.

## Language

`go`

## How to Run

1. Save the code as `main.go`.
2. Open a terminal in the same directory.
3. Run `go run main.go`.

## Original Article

This example accompanies the Turkish article: [Go ile Hibrit Docker Orkestratörü Geliştirmek: Tekil VM'den Çoklu Düğümlü Kümeye Yolculuk](https://fatihsoysal.com/blog/go-ile-hibrit-docker-orkestratoru-gelistirmek-tekil-vmden-coklu-dugumlu-kumeye-yolculuk/).

## License

MIT — see [LICENSE](LICENSE).
