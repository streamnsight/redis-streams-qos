# Simple Quality of Service (QoS) system using Redis Streams

## What is QoS?

Quality of Service is important in multi-tenant applications, in order to insure
each tenant has the same access to resources

In a typical flow, there may be jobs that need to be processed asynchronously, queued by each tenant

If tenant A queues up 1000 jobs all at once, and then tenant B queues up 1 job,
in a standard queueing system, tenant B will have to wait until all 1000 jobs from tenant A
have been processed.

In the contrary, a QoS system insures that jobs from tenant A and tenant B are processed in a round-robin fashion,
 that is, each tenant has its own job queue, and worker pick jobs up from each queue in rotation.
 
 
## How does QoS work?

To achieve this, 2 types of streams are used:

- a stream (queue) per tenant, where each tenant queues up its own jobs.
- a global stream, which aggregates jobs from all tenant streams, from which 
the workers pull jobs to process.

In between these streams, a process is in charge of monitoring the global job queue
and pull jobs from the tenant streams to forward to the global stream as workers are available to process the jobs.

## Why not forward jobs from each tenant stream to the global stream directly?

Let's assume a job takes 1s to process.

If tenant A queues up 1000 jobs in its own stream, and tenant B queues up 1 job just after, if the merger service was to simply forward
the jobs to the global stream, even when picking one job at a time from each tenant stream, the 1000 jobs from tenant A that arrived first 
would be forwarded to the global stream right away:

the merger service would fetch 1 jobs in tenant A stream and find 1, then fetch from tenant B stream and find none, until all 1000 jobs from tenant A are forwarded,
and the job from tenant B arrives in its own stream. At that point the merger service would find no more jobs in the tenant A stream and 1 job in tenant B stream.

That doesn't work.

For this to work, the merging service needs to monitor the global job queue, to keep it filled with as many jobs as consumers that are able to pick those up next.

This way, jobs are effectively queued up in the tenant stream, and then only the next N jobs are queued up for consumers to process.
When a consumer is done and free, it acknowledges the message in the global stream and removes it from the queue, then picks up its next job. 
That job is technically 'pending', and counts are being in the queue. The merger service checks the queue and sees there is a slot opening, 
and fetches a job from each tenant stream to feed to the global stream.

The global stream queue length varies, but is locked until new openings are cleared.

## Example

To run the example, run

```bash
docker-compose up -d
```

This creates 2 producers (ID 0 and 2)

Each producer sends a batch of messages of length (3* their numerical ID)
- producer 0 sends 1 messages at a time, every 1sec
- producer 2 sends 100 messages at a time, every 3sec

- the consumer is set to take 30ms to process the message, so it barely keeps up with the flow of messages

You can check in the logs of each container that the messages from the producer 0
are being processed every second or so, as expected.


```bash
docker logs -f <container_id>
```

use:
```bash
docker ps
```

to check container ids.




## TODO

The merger checks for number of consumers, and streams when starting up, but not after,
so if a consumer is removed / shut down, it should be un-registered from the stream.
