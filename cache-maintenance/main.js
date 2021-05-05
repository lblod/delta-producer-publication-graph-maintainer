import { produceMandateesDelta } from './producer';
import { sparqlEscapeUri } from 'mu';
import { CACHE_GRAPH,
       } from '../env-config';
import {  updateSudo as update } from '@lblod/mu-auth-sudo';
import { serializeTriple, storeError } from '../lib/utils';
import { chain } from 'lodash';

//TODO: consider bringing the processing of cache under a job operation.
// It feels a bit like over kill right now to do so.
export async function updateCacheGraph( deltaPayload ){
  try {
    const delta = await produceMandateesDelta(deltaPayload);

    //always first delet then insert
    const deletes = chain(delta)
          .map(c => c.deletes)
          .flatten()
          .map(t => serializeTriple(t))
          .value();

    const inserts = chain(delta)
          .map(c => c.inserts)
          .flatten()
          .map(t => serializeTriple(t))
          .value();

    if(deletes.length){
      await removeFromCacheGraph(deletes.join('\n'));
    }

    if(inserts.length){
      await insertIntoCacheGraph(inserts.join('\n'));
    }

  }
  catch(error){
    const errorMsg = `Error while processing delta ${error}`;
    console.error(errorMsg);
    await storeError(errorMsg);
  }
}

async function insertIntoCacheGraph( triples ){
  const queryStr = `
    INSERT DATA {
      GRAPH ${sparqlEscapeUri(CACHE_GRAPH)}{
       ${triples}
     }
    }
  `;

  await update(queryStr);
}

async function removeFromCacheGraph( triples ){
  const queryStr = `
    DELETE DATA {
      GRAPH ${sparqlEscapeUri(CACHE_GRAPH)}{
       ${triples}
     }
    }
  `;

  await update(queryStr);
}
