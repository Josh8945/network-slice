# Milestones 1–4

## Milestone 1
Two computational workers, controller, REST communication, process creation, scheduling, resource allocation and CPU/RAM monitoring.

## Milestone 2
Distributed workload execution with throughput, latency, jitter, task-failure/packet-loss percentage and resource measurements.

## Milestone 3
Controller/worker service decomposition and Client → Core/Controller → Edge/Workers deployment.

## Milestone 4
Bully leader election (ELECTION / OK / COORDINATOR messages, worst-case message count measured, consistency checked across nodes), Lamport logical clock, synchronization delay.

## Deployment
Docker Compose: one container per node on a private bridge network, addresses from environment variables. The controller calls workers with async httpx and queries all workers in parallel. Tasks fail over to the next worker if one is down.

## Scope
Prototype for the first four milestones; not a production telecom-grade system.
