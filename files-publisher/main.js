import DeltaCache from './delta-cache';
import { storeError } from '../lib/utils';
import {LOG_INCOMING_DELTA} from "../env-config";

const cache = new DeltaCache();
let hasTimeout = null;

export async function publishDeltaFiles(service_config, delta, generateOntheSpot = false ){
  if((delta.inserts.length || delta.deletes.length)){
    if (LOG_INCOMING_DELTA) {
      console.log(`Receiving delta ${JSON.stringify(delta)}`);
    }

    if(generateOntheSpot) {
      cache.push(delta);
      await cache.generateDeltaFile(service_config);
    }
    else {
      const processDelta = async function() {
        try {

          if (service_config.logOutgoingDelta) {
            console.log(`Pushing onto cache ${JSON.stringify(delta)}`);
          }

          cache.push( delta );

          if( !hasTimeout ){
            triggerTimeout(service_config);
          }
        }
        catch(e){
          console.error(`General error processing delta ${e}`);
          await storeError(service_config, e);
        }
      };
      processDelta();  // execute async to batch published data in files
    }
  }
}

export async function getDeltaFiles(service_config, since ){
  since = since || new Date().toISOString();
  const files = await cache.getDeltaFiles(service_config, since);
  return files;
}

function triggerTimeout(service_config){
  setTimeout( () => {
    try {
      hasTimeout = false;
      cache.generateDeltaFile(service_config);
    }
    catch(e){
      console.error(`Error generating delta file ${e}`);
      storeError(e);
    }
  }, service_config.deltaInterval );
  hasTimeout = true;
}

// TODO write the in-memory delta cache to a file before shutting down the service
