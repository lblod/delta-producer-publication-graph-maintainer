import DeltaFilesManager from './delta-delta-files-manager';
import { storeError } from '../lib/utils';
import { LOG_INCOMING_DELTA } from "../env-config";

export default class DeltaPublisher {

  constructor(serviceConfig) {
    this.serviceConfig = serviceConfig;
    this.deltaStreamName = this.serviceConfig.name;
    this.deltaFilesManager = new DeltaFilesManager();
    this.cache = [];
    this.hasTimeout = null;

    console.log(`Initialized delta publisher for delta stream: ${this.deltaStreamName}`);
  }

  async publish() {
    const deltas = this.cache.map(d => d.deltas);
    const callbacks = this.cache.map(d => d.updatePublicationGraphCallback);

    // flush cache, the next calls are async so data might come in while the procedure is ongoing
    this.cache = [];

    // publish the files
    await this.deltaFilesManager.generateDeltaFiles(this.serviceConfig, deltas);
    console.log(`Published delta files for: ${this.deltaStreamName}`);

    // update the publication graph
    for(const callback of callbacks) {
      await callback();
    }
    console.log(`Updated the publication graph for: ${this.deltaStreamName}`);
  }

  async schedulePublication(dataToPublish, generateOntheSpot = false ){
    if((dataToPublish.deltas.inserts.length || dataToPublish.deltas.deletes.length)){
      console.log(`Scheduling delta files publication for: ${this.deltaStreamName}`);

      this.cache.push(dataToPublish);

      if (LOG_INCOMING_DELTA) {
        console.log(`Receiving delta ${JSON.stringify(dataToPublish.deltas)}`);
      }

      if(generateOntheSpot) {
        await this.publish();
        console.log(`Published data (on the spot) for: ${this.deltaStreamName}`);
      }
      else {
        const processDelta = async function(publisherInstance) {
          try {

            if (publisherInstance.serviceConfig.logOutgoingDelta) {
              console.log(`Pushing onto cache ${JSON.stringify(dataToPublish.deltas)} for: ${publisherInstance.deltaStreamName}`);
            }

            if( !publisherInstance.hasTimeout ){
              publisherInstance.triggerTimeout();
            }
          }
          catch(e){
            console.error(`General error processing delta ${e}`);
            await storeError(publisherInstance.serviceConfig, e);
          }
        };
        processDelta(this);  // execute async to batch published data in files
      }
    }
  }

  async getDeltaFiles(since){
    // TODO: move this outside of the publisher somehow
    since = since || new Date().toISOString();
    const files = await this.deltaFilesManager.getDeltaFiles(this.serviceConfig, since);
    console.log(`Retreived ${files?.length} for: ${this.deltaStreamName}`);
    return files;
  }

  /**
   * Triggers a timeout for batch processing of delta changes into files.
   */
  async triggerTimeout(){
    setTimeout(async () => {
      try {
        this.hasTimeout = false;
        await this.publish();
        console.log(`Published data (scheduled) for: ${this.deltaStreamName}`);
      }
      catch(e){
        console.error(`Error generating delta file ${e}`);
        await storeError(this.serviceConfig, e);
      }
    }, this.serviceConfig.deltaInterval );
    this.hasTimeout = true;
  }
  // TODO write the in-memory delta cache to a file before shutting down the service
}
