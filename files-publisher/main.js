import {
    DELTA_INTERVAL, LOG_INCOMING_DELTA,
    LOG_OUTGOING_DELTA
} from '../env-config';
import DeltaCache from './delta-cache';
import { storeError } from '../lib/utils';

const cache = new DeltaCache();
let hasTimeout = null;

export async function publishDeltaFiles( delta ){
  if((delta.inserts.length || delta.deletes.length)){
    if (LOG_INCOMING_DELTA) {
      console.log(`Receiving delta ${JSON.stringify(delta)}`);
    }

    const processDelta = async function() {
      try {

        if (LOG_OUTGOING_DELTA) {
          console.log(`Pushing onto cache ${JSON.stringify(delta)}`);
        }

        cache.push( delta );

        if( !hasTimeout ){
          triggerTimeout();
        }
      }
      catch(e){
        console.error(`General error processing delta ${e}`);
        await storeError(e);
      }
    };
    processDelta();  // execute async to batch published data in files
  }
}

export async function getDeltaFile( since ){
  since = since || new Date().toISOString();
  const files = await cache.getDeltaFiles(since);
  return files;
}

function triggerTimeout(){
  setTimeout( () => {
    try {
      hasTimeout = false;
      cache.generateDeltaFile();
    }
    catch(e){
      console.error(`Error generating delta file ${e}`);
      storeError(e);
    }
  }, DELTA_INTERVAL );
  hasTimeout = true;
}

// TODO write the in-memory delta cache to a file before shutting down the service
