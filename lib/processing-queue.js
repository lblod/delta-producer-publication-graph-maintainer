import { storeError } from './utils';
import { QUEUE_POLL_INTERVAL } from '../env-config';

export class ProcessingQueue {
  constructor( name = "default-processing-queue") {
    this.name = name;
    this.queue = [];
    this.run();
    this.executing = false; //This is useful for tracking state.
  }

  async run() {
    if (this.queue.length > 0 && !this.executing) {
      try {
        this.executing = true;
        console.log(`${this.name}: Executing oldest task on queue`);
        await this.queue.shift()();
        console.log(`${this.name}: Remaining number of tasks ${this.queue.length}`);
      }
      catch(error){
        const errorMsg = `${this.name}: Error while processing delta in queue ${error}`;
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
