import DeltaCache from './delta-cache';
import { storeError } from '../lib/utils';
import {LOG_INCOMING_DELTA} from "../env-config";

const cache = new DeltaCache();
let hasTimeout = null;

export async function publishDeltaFiles(serviceConfig, delta ){
  if((delta.inserts.length || delta.deletes.length)){
    if (LOG_INCOMING_DELTA) {
      console.log(`Receiving delta ${JSON.stringify(delta)}`);
    }

    const processDelta = async function() {
      try {

        if (serviceConfig.logOutgoingDelta) {
          console.log(`Pushing onto cache ${JSON.stringify(delta)}`);
        }

        cache.push( delta );

        if( !hasTimeout ){
          triggerTimeout(serviceConfig);
        }
      }
      catch(e){
        console.error(`General error processing delta ${e}`);
        await storeError(serviceConfig, e);
      }
    };
    processDelta();  // execute async to batch published data in files
  }
}

export async function getDeltaFiles(serviceConfig, since ){
  since = since || new Date().toISOString();
  const files = await cache.getDeltaFiles(serviceConfig, since);
  return files;
}

function triggerTimeout(serviceConfig){
  setTimeout( () => {
    try {
      hasTimeout = false;
      cache.generateDeltaFile(serviceConfig);
    }
    catch(e){
      console.error(`Error generating delta file ${e}`);
      storeError(e);
    }
  }, serviceConfig.deltaInterval );
  hasTimeout = true;
}

// TODO write the in-memory delta cache to a file before shutting down the service
