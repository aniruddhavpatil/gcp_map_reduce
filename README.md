# Map Reduce on the Google Cloud Platform
## Main Entities
1. Store
1. Master
1. Worker
1. Cloud Interface
### Store
1. Store Server
### Master
1. RPC Server
1. Store Client
### Worker
1. RPC Client

### Cloud Interface
1. Instance information and management

## Execution Flow
1. The map-reduce Master is already running on a VM.
1. Key-Value store is also running on another VM.
1. Master first launches required number of mapper VMs.
1. Map-tasks are executed.
1. Map tasks are terminated by the master once complete.
1. Reducer tasks are executed.
1. Once reducers have finished, master terminates all reducer VMs.

## Fault Tolerance Mechanisms
### Heatbeat (Against preemptions)
Each worker calls the heartbeat() method via RPC that runs on another thread. This method is served by the Master’s RPC Server. If the Master does not receive an entity’s heartbeat, it considers it dead and attempts the appropriate resurrection:
If the dead VM is a mapper, restart it.
If the dead VM is a reducer, restart all reducers.
### Fault Handler (Against failed workers)
The RPC Server on the Master serves a fault() method that can be called upon encountering a fault by the Worker process. The Master then takes the appropriate action based on the task_type and task_id.

## Infrastructure
### Choice of VMs
All experiments have been done with the e2-micro machine with minimal ubuntu 20.04 LTS. Experiments at the beginning were done with non-preemptible VMs. More recent experiments have been conducted usin pre-emptible VMs.
### Limiting Bursting
To avoid the phenomenon of bursting, the Cloud Interface waits after issuing the current operation (issuing a call to starting an instance) for a set period of time (1sec) before issuing the next operation. I incorporated this as a result of encountering the RESOURCE_OPERATION_RATE_EXCEEDED error.
### Performance
I used a small corpus to make the testing faster.
Word Count (3 mappers, 5 reducers)
Non-preemptive: Average time from issuing job to the Master till obtaining output: ~3 minutes
Preemptive: Average time from issuing job to the Master till obtaining output: ~7 minutes
Inverted Index (4 mappers, 4 reducers)
Non-preemptive: Average time from issuing job to the Master till obtaining output: ~5 minutes
Preemptive: Average time from issuing job to the Master till obtaining output: ~12 minutes
