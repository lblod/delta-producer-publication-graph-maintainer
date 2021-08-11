import { storeError } from './utils';
import { QUEUE_POLL_INTERVAL } from '../env-config';

export class ProcessingQueue {
  constructor() {
    this.queue = [];
    this.run();
    this.executing = false; //To avoid subtle race conditions TODO: is this required?
  }

  async run() {
    if (this.queue.length > 0 && !this.executing) {
      try {
        this.executing = true;
        console.log("Executing oldest task on queue");
        await this.queue.shift()();
        console.log(`Remaining number of tasks ${this.queue.length}`);
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
      setTimeout(() => { this.run(); }, QUEUE_POLL_INTERVAL);
    }
  }

  addJob(origin) {
    this.queue.push(origin);
  }
}
