import { storeError } from './utils';

export class ProcessingQueue {
  constructor(serviceConfig, name = "default-processing-queue") {
    this.name = name;
    this.queue = [];
    this.run();
    this.executing = false; //This is useful for tracking state.
    this.queuePollInterval = serviceConfig.queuePollInterval;
    this.serviceConfig = serviceConfig;
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
        await storeError(this.serviceConfig, errorMsg);
      }
      finally {
        this.executing = false;
        this.run();
      }
    }
    else {
      setTimeout(() => { this.run(); }, this.queuePollInterval);
    }
  }

  addJob(origin) {
    this.queue.push(origin);
  }
}
