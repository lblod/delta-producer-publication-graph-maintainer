//TODO: error cathcin
export class ProcessingQueue {
  constructor() {
    this.queue = [];
    this.run();
  }

  async run() {
    console.log(`Number of queued jobs: ${this.queue.length}`);
    if (this.queue.length > 0) {
      console.log("executing oldest task on queue");
      await this.queue.shift()();
      this.run();
    }
    else {
      setTimeout(() => {this.run();}, 3000);
    }
  }

  addJob(origin) {
    this.queue.push(origin);
  }
}
