use async_channel::RecvError;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};

enum SignalState {
    Job(String),
}

async fn worker(
    worker_id: i32,
    work_receiver_channel: async_channel::Receiver<SignalState>,
    result_sender_channel: mpsc::Sender<String>,
) {
    println!("Worker-[{worker_id}]: Spawned, waiting for jobs.");

    loop {
        match work_receiver_channel.recv().await {
            Ok(job) => match job {
                SignalState::Job(filename) => {
                    // Simulate some fake work.
                    sleep(Duration::from_secs(5)).await;
                    let completed_task = format!("Worker-[{worker_id}]: Completed task {filename}");
                    let _ = result_sender_channel.send(completed_task).await;
                }
            },
            Err(e) => match e {
                RecvError => {
                    println!("Worker-[{worker_id}]: Terminating, no more messages remaining? {e}",);
                    // Dropped it here, because we know that at this point there will be
                    // no more messages published on the clone of this sender-channel.
                    drop(result_sender_channel);
                    break;
                }
            },
        }
    }

    println!("Worker-[{worker_id}]: Terminated");
}

#[tokio::main]
async fn main() {
    let max_worker = 5;
    let max_tasks = 10;

    let (work_sender_channel, work_receiver_channel) = async_channel::bounded::<SignalState>(1);
    let (result_sender_channel, mut result_receiver_channel) = mpsc::channel::<String>(max_tasks);

    // Let us spawn max_workers
    let mut worker_handles = Vec::with_capacity(max_worker);
    for worker_id in 0..max_worker {
        let work_channel = work_receiver_channel.clone();
        let result_send_channel = result_sender_channel.clone();

        // Optionally push to an array of handles, not required though.
        worker_handles.push(tokio::spawn(async move {
            worker(worker_id as i32, work_channel, result_send_channel).await;
        }));
    }

    // Send arbitrary amount of jobs, here we are sending numbers from 1 to 10
    // This will also block until a worker has picked up a job
    // because the size of channel is 1
    for idx in 0..max_tasks {
        let fname = format!("somefile-{idx}");
        let _ = work_sender_channel.send_blocking(SignalState::Job(fname));
    }

    // If we reached here, all the jobs were successfully published, lets close the channel.
    // This will make the work_receiver_channel.recv() return
    drop(work_sender_channel);

    // Drop the original result_sender_channel, the clones will be dropped
    // from inside the tasks, once the task sees no new messages to process.
    drop(result_sender_channel);

    while let Some(fname) = result_receiver_channel.recv().await {
        println!("Got message {fname}");
        let mut file = File::create(fname).await.unwrap();
        file.write_all(b"hello, world!").await.unwrap();
    }
}
