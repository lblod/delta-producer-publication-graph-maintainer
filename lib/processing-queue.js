import { storeError } from './utils';

export class ProcessingQueue {
  constructor() {
    this.queue = [];
    this.run();
    this.executing = false; //To avoid subtle race conditions
  }

  async run() {
    console.log(`Number of queued jobs: ${this.queue.length}`);
    if (this.queue.length > 0 && !this.executing) {
      try {
        this.executing = true;
        console.log("executing oldest task on queue");
        await this.queue.shift()();
        this.run();
      }
      catch(error){
        const errorMsg = `Error while processing delta in queue ${error}`;
        console.error(errorMsg);
        await storeError(errorMsg);
      }
      finally {
        this.executing = false;
      }
    }
    else {
      setTimeout(() => { this.run(); }, 3000);
    }
  }

  addJob(origin) {
    this.queue.push(origin);
  }
}
