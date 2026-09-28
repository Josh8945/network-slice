 Distributed Network Slicing Platform

A distributed computing prototype designed to demonstrate the creation, management, monitoring, processing, and coordination of network slices across multiple computational nodes. The platform is being developed as an evolving telecommunications-oriented distributed system using a controller-worker architecture.

The current implementation focuses on the first four milestones of the project: Distributed Operating System Foundation, Distributed Processing and Performance, Distributed Architecture, and Distributed Algorithms and Coordination.

🖥️ Distributed Node Resource Allocation Blueprint — Milestone 1 Foundation

The initial distributed environment uses three Ubuntu virtual machines running under Oracle VM VirtualBox. The nodes form the computational foundation of the platform.

🏢 1. Controller / Master Node (master)

Role: Distributed Controller / Scheduler / Resource Manager

Operating System: Ubuntu 26.04.1 LTS

IP Address: 192.168.2.113

Hostname: master

Primary Responsibilities:

Network-slice request handling

Worker selection and scheduling

Resource allocation

Distributed coordination

Performance monitoring

🖥️ 2. Worker Node 1 (node1)

Role: Distributed computation / task execution node

Operating System: Ubuntu 26.04.1 LTS

IP Address: 192.168.2.110

Hostname: node1

Primary Responsibilities:

Process execution

Slice resource allocation

CPU and memory monitoring

Distributed workload processing

🖥️ 3. Worker Node 2 (node2)

Role: Distributed computation / task execution node

Operating System: Ubuntu 26.04.1 LTS

IP Address: 192.168.2.111

Hostname: node2

Primary Responsibilities:

Process execution

Slice resource allocation

CPU and memory monitoring

Distributed workload processing

🌐 Network Communication

The three virtual machines communicate through the local network using IP connectivity and hostname resolution.

Application-level communication is implemented using REST/HTTP rather than relying on SSH as the system control-plane mechanism.

                         +----------------------+
                         |       CLIENT         |
                         +----------+-----------+
                                    |
                              REST / HTTP
                                    |
                                    v
                         +----------------------+
                         |   MASTER / CONTROLLER |
                         |     192.168.2.113     |
                         +----------+-----------+
                                    |
                         REST / HTTP |
                    +---------------+---------------+
                    |                               |
                    v                               v
          +--------------------+          +--------------------+
          |      NODE 1        |          |      NODE 2        |
          |   192.168.2.110    |          |   192.168.2.111    |
          +--------------------+          +--------------------+

🛠️ Milestone 1 — Distributed Operating System Foundation

Milestone 1 establishes the computational foundation of the distributed platform.

Implemented Concepts

Multiple computational nodes

Controller-worker architecture

Inter-node REST communication

Process creation

Basic process scheduling

Resource allocation

CPU and memory monitoring

Logical representation of network slices

Network Slice Resource Model

A slice request is represented using:

Slice ID
CPU requirement
Memory requirement
Bandwidth requirement
Priority

The controller evaluates worker resource availability and selects an eligible worker using resource-aware scheduling.

Process Model

Client Request
      |
      v
Controller
      |
      v
Scheduler
      |
      v
Worker Selection
      |
      v
Worker Process
      |
      v
Task Result

📊 Milestone 2 — Distributed Processing and Performance

Milestone 2 extends the prototype by distributing computational workloads across multiple worker nodes and measuring system performance.

Performance Metrics

The platform records:

Throughput

Latency

Jitter

Packet loss / task failure rate

CPU utilization

Memory utilization

Worker load distribution

Performance Calculations

Throughput

Throughput = Completed Tasks / Total Execution Time

Latency

Latency = Response Time - Request Time

Jitter

The prototype estimates jitter using the statistical variation of measured response times.

Packet Loss / Failure Rate

Failure Rate (%) =
(Failed Tasks / Total Tasks) × 100

Results

Performance results are automatically stored in:

results/milestone_results.json

This provides a structured dataset that can later be used for further analysis and visualization.

📐 Milestone 3 — Distributed Architecture Design

Milestone 3 transforms the initial prototype into a more clearly separated distributed architecture.

Architectural Components

The platform separates responsibilities into:

Client/API interface

Controller

Slice Manager

Scheduler

Resource Manager

Worker Services

Resource Monitoring

Coordination

Deployment Architecture

The current design can be represented as:

                 CLIENT
                    |
                    v
             CORE / CONTROLLER
          +---------------------+
          | Slice Manager       |
          | Scheduler           |
          | Resource Manager    |
          | Coordinator         |
          +----------+----------+
                     |
              +------+------+
              |             |
              v             v
           EDGE          EDGE
         WORKER 1      WORKER 2

Architectural Evolution

The architecture provides a foundation for future development toward an Edge ↔ Core ↔ Cloud model.

The controller performs centralized coordination and scheduling, while computational tasks are distributed to worker nodes closer to the execution layer.

🕒 Milestone 4 — Distributed Algorithms and Coordination

Milestone 4 introduces mechanisms for coordination and distributed state management.

1. Distributed Coordination

The platform demonstrates coordination between the controller and worker nodes through application-level messages.

2. Leader Election

The Bully algorithm elects a coordinator. Every node has a numeric ID (controller=1, node1=2, node2=3):

1. The initiator sends ELECTION to every node with a higher ID.
2. Any live higher node replies OK and starts its own election.
3. A node that gets no OK wins and broadcasts COORDINATOR to all nodes.

Message complexity is O(n²) in the worst case (lowest ID starts): n(n−1)/2 ELECTION + (n−1) COORDINATOR messages, which is 5 for our 3 nodes. The demo starts from the lowest ID, so it measures that worst case. It also checks consistency, meaning every node reports the same leader. If node2 is stopped, a new election picks node1.

3. Logical Clocks

A Lamport-style logical clock is used to demonstrate distributed event ordering.

The basic rules are:

1. Before a local event:
   L = L + 1

2. When receiving a message with timestamp T:
   L = max(L, T) + 1

This allows distributed events to be ordered without depending exclusively on synchronized physical clocks.

4. Coordination Measurements

The prototype records:

Coordination messages

Logical clock values

Synchronization delay

Distributed event acknowledgements

🧪 Live Demonstration

The complete local prototype can be executed from VS Code.

Install Dependencies

pip install -r requirements.txt

Run the Demonstration

python run_demo.py

The demonstration automatically starts:

Controller
Worker 1
Worker 2

and executes demonstrations corresponding to Milestones 1–4.

Expected output includes:

MILESTONE 1: ✓ distributed foundation
MILESTONE 2: ✓ throughput / latency / jitter / packet loss
MILESTONE 3: ✓ distributed architecture
MILESTONE 4: ✓ leader election / logical clock

Detailed experimental results are saved to:

results/milestone_results.json

🐳 Docker Demonstration

Each node runs in its own container on a private bridge network (`telecom_net`). The nodes find each other by service name through Docker's DNS, so no IP address is hardcoded. All addresses come from environment variables in `docker-compose.yml` (`WORKER_NODES`, `PEERS`, `NODE_ID`).

docker compose up -d --build      # build the image, start controller + node1 + node2
docker compose ps                 # all three should be "healthy"
python run_demo.py --docker       # milestones 1-4 + failure demo
docker compose down               # stop everything

The `--docker` run adds a failure demonstration (Failure → Detection → Recovery → Service Restoration):
it stops node2, the controller detects it offline, the Bully election picks node1, tasks fail over to node1,
then node2 is restarted and MTTR is measured. Results are saved to results/docker_results.json.

Manual failure demo in class: `docker stop node2`, then POST http://localhost:8000/coordination/election
(or use the Swagger UI at http://localhost:8000/docs), then `docker start node2`.

📁 Repository Structure

Network_Slicing_Platform/
│
├── controller/
│
├── workers/
│   ├── node1/
│   └── node2/
│
├── communication/
│
├── monitoring/
│
├── tests/
│
├── docs/
│   ├── ARCHITECTURE.md
│   ├── PROCESS_MODEL.md
│   ├── RESOURCE_MODEL.md
│   └── MILESTONES.md
│
├── results/
│   └── milestone_results.json
│
├── requirements.txt
├── run_demo.py
├── service.py
└── README.md

⚙️ Technology Stack

Technology

Purpose

Python

Distributed application implementation

FastAPI

REST API services

Uvicorn

Application server

Requests

Inter-service communication

psutil

CPU and memory monitoring

Python multiprocessing

Process creation and task execution

Ubuntu

Distributed node operating system

Oracle VM VirtualBox

Virtualized node environment

Git / GitHub

Source-code version control

Planned Technologies

As the platform evolves, additional technologies may be incorporated for:

Persistent telemetry storage

Event streaming

Distributed analytics

Performance dashboards

Containerized slice isolation

Failure recovery

Large-scale distributed processing

Potential technologies include PostgreSQL/TimescaleDB, Apache Kafka, Apache Spark, Docker, Power BI, and Tableau.

📈 Project Evolution

The platform is designed as a continuously evolving distributed system rather than a collection of unrelated implementations.

Distributed Prototype
        ↓
Distributed System
        ↓
Reliable Distributed System
        ↓
Telecom-Oriented Distributed System

The first four milestones establish the foundation for subsequent development involving scalability, reliability, failure recovery, advanced coordination, telemetry, and telecom-oriented resource management.

📚 Documentation

Additional project documentation is available in the docs/ directory:

ARCHITECTURE.md — Distributed architecture

PROCESS_MODEL.md — Process creation and execution model

RESOURCE_MODEL.md — Resource allocation model

MILESTONES.md — Milestone implementation summary

🎓 Academic Project

This repository documents the development of a distributed network-slicing platform for a distributed computing and telecommunications project.

The implementation currently focuses on demonstrating the distributed-system principles required by the first four development milestones while providing an extensible foundationrun 
