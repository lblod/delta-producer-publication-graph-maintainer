import DeltaCache from './delta-cache';
import { storeError } from '../lib/utils';
import { LOG_INCOMING_DELTA } from "../env-config";

/**
 * Manages the publication of delta files, i.e. the insertions and deletions.
 * Utilizes a delta cache to store these changes before generating and publishing delta files;
 *  this optimises the number of files it creates.
 */
export default class DeltaPublisher {

  constructor(serviceConfig) {
    this.serviceConfig = serviceConfig;
    this.deltaStreamName = this.serviceConfig.name;
    this.deltaCache = new DeltaCache();
    this.hasTimeout = null;

    console.log(`Initialized delta publisher for delta stream: ${this.deltaStreamName}`);
  }

  /**
   * Publishes delta changes by either immediately generating a delta file or scheduling it for later.
   * @param {Object} delta - Object containing the delta changes with `inserts` and `deletes`.
   * @param {boolean} [generateOntheSpot=false] - Whether to generate the delta file immediately.
   */
  async publishDeltaFiles(delta, generateOntheSpot = false ){
    if((delta.inserts.length || delta.deletes.length)){
      console.log(`Scheduling delta files publication for: ${this.deltaStreamName}`);

      if (LOG_INCOMING_DELTA) {
        console.log(`Receiving delta ${JSON.stringify(delta)}`);
      }

      if(generateOntheSpot) {
        this.deltaCache.push(delta);
        await this.deltaCache.generateDeltaFile(this.serviceConfig);
        console.log(`Published delta files (on the spot) for: ${this.deltaStreamName}`);
      }
      else {
        const processDelta = async function(publisherInstance) {
          try {

            if (publisherInstance.serviceConfig.logOutgoingDelta) {
              console.log(`Pushing onto cache ${JSON.stringify(delta)} for: ${publisherInstance.deltaStreamName}`);
            }

            publisherInstance.deltaCache.push(delta);

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

  /**
   * Retrieves delta files generated since the specified timestamp, up to a maximum of MAX_DELTA_FILES_PER_REQUEST.
   * @param {string} [since=new Date().toISOString()] - Timestamp to fetch delta files from.
   * @returns {Promise<Array>} Collection of delta files.
   */
  async getDeltaFiles(since){
    since = since || new Date().toISOString();
    const files = await this.deltaCache.getDeltaFiles(this.serviceConfig, since);
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
        await this.deltaCache.generateDeltaFile(this.serviceConfig);
        console.log(`Published delta files (scheduled) for: ${this.deltaStreamName}`);
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
