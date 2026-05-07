- Threads are a solution to implement **Concurrency & Parallelism**.
- Disregarding the implementation, it's a theoretical way of **isolating logic and functionality**. 
- A **Process** may contain many threads.
- A **Process** is a program running on a machine **isolated** from other processes having its **own resources** (like assigned memory, stack, etc) and its **own address space**. 
- **Parallelism** is when you have multiple tasks **running in parallel** and **splitting resources** evenly or unevenly (like ) .
- Concurrency on the other hand is when you have a **SINGLE** set of resources and you **switch the context** from one task to another by some algorithm that ultimately serves all the tasks evenly or unevenly.
- Each thread is **isolated** conceptually even though it is practically **sharing an address space** (and shared memory) with other threads and could actually access another stack if it knew the right address.
- Each thread has its **own** stack, its **own** memory (set of registers), program counter, etc. However, it can send and receive data to and from other threads either through pipe channels or just a centralized shared memory architecture.
- Threads can be used to fire off background periodic jobs with **low overhead** by implementing a loop that sleeps for a period of time and then check or execute a task.
- Asynchronous programming (also called event-driven programming) on the other hand consists of **ONE** thread that executes the main task while always listening for events and switches to different tasks typically when an event occurs. 
- **Asynchronous programming** could be particularly useful for **I/O concurrency** (like JS in web apps) where there are a lot of requests and a lot of **waiting** for responses.
- Some thread instructions are atomic and some are not (will explore later).
- There are some challenges of threads sharing memory:
	1. **Race conditions**
		- It is when 2 or more threads are executing on the **same piece** of data on the **same time**.
		- One solution of race conditions is **Mutex Locks** (Mutex stands for mutually exclusive).
		- Usually, some code is protected by this lock (there can be infinitely many locks) on different threads. The first thread to acquire the lock gets to execute the code wrapped with the lock and all other code wrapped by the same lock has to wait until the lock is available again.
	2. **Coordination between threads**:
		- Sometimes, you'd want the different threads to actually interact with each other and cooperate
		- Solutions include:
			1. **Channels** (usually between Master and Worker threads or rarely between worker threads)
			2. **Sync Conditions & Wait Groups** (basically a **Semaphore** implementation in GO) (utilizes shared data)
	3. **Deadlocks**
		- When a thread is waiting on a lock which is acquired by another thread and that other thread is waiting on a different lock which is acquired by the first thread forming an **infinite waiting cycle**. 
		- For example, if thread 1 acquires lock A and thread 2 acquires lock B, then after that thread 1 wants to acquire lock B and thread 2 wants to acquire lock A. Neither thread will release their locks as they're not done and the condition to resume and finish is to acquire the other lock which will never also be released.

### Web Crawler (Practical Example)

1. **Serial Crawler**
	- Basically going through the urls one by one and then searching in that url recursively (no Concurrency applied)
	
``` go
//
// Serial crawler
//

func Serial(url string, fetcher Fetcher, fetched map[string]bool) {
	//shared map
	if fetched[url] {
		return
	}
	fetched[url] = true
	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	for _, u := range urls {
		Serial(u, fetcher, fetched)
	}
}
```


2. **Concurrent Mutex Crawler**
	- Coordination is achieved through shared data.
	- New Threads are created from a graph of threads (which leads to the number of treads created being unbounded)

```go
//
// Concurrent crawler with shared state and Mutex

// shared state with shared lock and map 
type fetchState struct {
	mu      sync.Mutex
	fetched map[string]bool
}

func (fs *fetchState) testAndSet(url string) bool {
	// the fetch checking process has to be atomic (mutex)
	fs.mu.Lock()
	// defer just ensures that the lock is released even if function fails or
	// returns abnormally
	defer fs.mu.Unlock()
	// we set it true in all cases and then return the old value
	r := fs.fetched[url]
	fs.fetched[url] = true
	return r
}

func ConcurrentMutex(url string, fetcher Fetcher, fs *fetchState) {
	// if it returns true then it's already checked and we return
	// if it returns false then it's state is set to true and we proceed with
	// fetching
	if fs.testAndSet(url) {
		return
	}
	urls, err := fetcher.Fetch(url)
	if err != nil {
		return
	}
	// WaitGroups are semaphores; Add increments - Done decrements
	var done sync.WaitGroup
	for _, u := range urls {
		// for every url we find we increment and assign a thread to the task
		done.Add(1)
		go func(u string) {
			ConcurrentMutex(u, fetcher, fs)
			// when the task is done, we decrement and exit
			done.Done()
		}(u)
	}
	// Wait waits until the counter reaches 0 then returns
	done.Wait()
}
```

3. **Concurrent Channel Crawler**
	- Coordination is achieved through Master-Worker communication using Channel pipes.
	- Threads are created only from the Coordinator (Master thread)

```go
//
// Concurrent crawler with channels

// the worker function which populates the channel through different threads
func worker(url string, ch chan []string, fetcher Fetcher) {
	urls, err := fetcher.Fetch(url)
	if err != nil {
		// empty string array
		ch <- []string{}
	} else {
		ch <- urls
	}
}

func coordinator(ch chan []string, fetcher Fetcher) {
	// counter used instead of semaphores since there is only 1 master thread
	n := 1
	fetched := make(map[string]bool)
	// keeps recieving from the channel and waiting when it's empty
	for urls := range ch {
		// loops over every set of urls from the channel
		for _, u := range urls {
			if fetched[u] == false {
				fetched[u] = true
				n += 1
				// creates a new thread (subroutine)
				go worker(u, ch, fetcher)
			}
		}
		// decrements after finishing
		n -= 1
		// only way to break the channel loop is manually through break
		// when n is 0 all threads have finished their tasks
		if n == 0 {
			break
		}
	}
}

func ConcurrentChannel(url string, fetcher Fetcher) {
	ch := make(chan []string)
	go func() {
		// we send the first url to the channel so it's not empty in the 
		// coordinator
		ch <- []string{url}
	}()
	coordinator(ch, fetcher)
}
```