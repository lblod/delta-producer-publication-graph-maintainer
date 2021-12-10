import { storeError } from './utils';
import { QUEUE_POLL_INTERVAL } from '../env-config';

export class ProcessingQueue {
  constructor() {
    this.queue = [];
    this.run();
    this.executing = false; //This is useful for tracking state.
  }

  async run() {
    if (this.queue.length > 0 && !this.executing) {
      try {
        this.executing = true;
        console.log("Executing oldest task on queue");
        await this.queue.shift()();
        console.log(`Remaining number of tasks ${this.queue.length}`);
      }
      catch(error){
        const errorMsg = `Error while processing delta in queue ${error}`;
        console.error(errorMsg);
        await storeError(errorMsg);
      }
      finally {
        this.executing = false;
        this.run();
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
