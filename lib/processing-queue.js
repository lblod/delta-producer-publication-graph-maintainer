import { storeError } from './utils';

export class ProcessingQueue {
  constructor(service_config, name = "default-processing-queue") {
    this.name = name;
    this.queue = [];
    this.run();
    this.executing = false; //This is useful for tracking state.
    this.queuePollInterval = 60000;

    if(service_config) {
      this.queuePollInterval = service_config.queuePollInterval;
      this.service_config = service_config;
    }
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
        if(this.service_config) {
          await storeError(this.service_config, errorMsg);
        }
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
