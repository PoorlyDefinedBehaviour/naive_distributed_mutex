use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Debug, Clone)]
struct AcquireRequest {
    process_id: u64,
    timestamp: u64,
}

#[derive(Debug)]
struct AcquireResponse {
    timestamp: u64,
}

struct Process {
    id: u64,
    requests: Mutex<Vec<AcquireRequest>>,
    clock: AtomicU64,
}

#[derive(Debug)]
struct NotAcquiredError;

impl Process {
    fn new(id: u64) -> Self {
        Self {
            id,
            requests: Mutex::new(Vec::new()),
            clock: AtomicU64::new(0),
        }
    }

    fn next_id(&self) -> u64 {
        self.clock.fetch_add(1, Ordering::SeqCst)
    }

    fn send_acquire(&self, processes: &[Arc<Process>]) -> Result<u64, NotAcquiredError> {
        {
            let requests = self.requests.lock().unwrap();
            if let Some(request) = requests
                .iter()
                .find(|request| request.process_id == self.id)
            {
                return Ok(request.timestamp);
            }
        }

        loop {
            let timestamp = self.next_id();

            let request = AcquireRequest {
                process_id: self.id,
                timestamp: timestamp,
            };

            let mut found_process_with_higher_timestamp = false;
            for process in processes {
                if process.id != self.id {
                    let response = process.receive_acquire(request.clone());
                    if response.timestamp > timestamp {
                        self.clock.store(response.timestamp + 1, Ordering::SeqCst);
                        found_process_with_higher_timestamp = true
                    }
                }
            }

            if found_process_with_higher_timestamp {
                continue;
            }

            let mut requests = self.requests.lock().unwrap();

            requests.push(request);

            return Ok(timestamp);
        }
    }

    fn send_release(&self, timestamp: u64, processes: &[Arc<Process>]) {
        for process in processes {
            // The process releases its own request.
            process.receive_release(timestamp, self.id);
        }
    }

    fn receive_acquire(&self, request: AcquireRequest) -> AcquireResponse {
        let current_timestamp = self.clock.load(Ordering::SeqCst);
        if request.timestamp < current_timestamp {
            return AcquireResponse {
                timestamp: current_timestamp,
            };
        }
        let mut requests = self.requests.lock().unwrap();

        requests.push(request);

        AcquireResponse {
            timestamp: self.next_id(),
        }
    }

    fn receive_release(&self, timestamp: u64, process_id: u64) {
        let mut requests = self.requests.lock().unwrap();

        requests.retain_mut(|request| {
            request.process_id != process_id && request.timestamp != timestamp
        });
    }

    fn is_resource_owner(process_id: u64, processes: &[Arc<Process>]) -> bool {
        processes.iter().all(|process| {
            let requests = process.requests.lock().unwrap();

            let oldest_request = match requests.iter().min_by_key(|request| request.timestamp) {
                None => return false,
                Some(v) => v,
            };

            let oldest_request_belongs_to_process = oldest_request.process_id == process_id;

            let requests_made_by_other_processes: HashSet<_> = requests
                .iter()
                .filter(|request| request.process_id != process_id)
                .map(|request| request.process_id)
                .collect();

            let every_process_has_requested_ownership =
                requests_made_by_other_processes.len() == processes.len() - 1;

            every_process_has_requested_ownership && oldest_request_belongs_to_process
        })
    }

    fn get_requests(&self) -> Vec<AcquireRequest> {
        self.requests.lock().unwrap().clone()
    }
}

fn main() {
    let processes = Arc::new([
        Arc::new(Process::new(0)),
        Arc::new(Process::new(1)),
        // Arc::new(Process::new(2)),
        // Arc::new(Process::new(3)),
        // Arc::new(Process::new(4)),
    ]);

    let handles: Vec<_> = processes
        .iter()
        .map(|process| {
            let processes = Arc::clone(&processes);
            let process = Arc::clone(process);

            std::thread::spawn(move || {
                for _ in 0..10 {
                    let duration = Duration::from_secs(3);
                    std::thread::sleep(duration);

                    if let Ok(timestamp) = process.send_acquire(processes.as_ref()) {
                        std::thread::sleep(duration);
                        if Process::is_resource_owner(process.id, processes.as_ref()) {
                            process.send_release(timestamp, processes.as_ref());
                        }
                    };
                }
            })
        })
        .collect();

    let processes = Arc::clone(&processes);
    let watcher_handle = std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(1));
        for process in processes.iter() {
            println!(
                "process_id={} is_resource_owner={} requests={:?}",
                process.id,
                Process::is_resource_owner(process.id, processes.as_ref()),
                process.get_requests(),
            );
        }
    });

    for handle in handles {
        handle.join().unwrap();
    }

    watcher_handle.join().unwrap();
}
