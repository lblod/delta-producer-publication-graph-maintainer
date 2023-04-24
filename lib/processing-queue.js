import { storeError } from './utils';

export class ProcessingQueue {
  constructor( _config, name = "default-processing-queue") {
    this.name = name;
    this.queue = [];
    this.run();
    this.executing = false; //This is useful for tracking state.
    this.QUEUE_POLL_INTERVAL = _config.QUEUE_POLL_INTERVAL;
    this._config = _config;
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
        await storeError(this._config, errorMsg);
      }
      finally {
        this.executing = false;
        this.run();
      }
    }
    else {
      setTimeout(() => { this.run(); }, this.QUEUE_POLL_INTERVAL);
    }
  }

  addJob(origin) {
    this.queue.push(origin);
  }
}
