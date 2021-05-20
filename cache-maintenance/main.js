import { produceConceptSchemeDelta } from './producer';
import { sparqlEscapeUri } from 'mu';
import { CACHE_GRAPH } from '../env-config';
import {  updateSudo as update } from '@lblod/mu-auth-sudo';
import { serializeTriple, storeError } from '../lib/utils';
import { chain } from 'lodash';

//TODO: consider bringing the processing of cache under a job operation.
// It feels a bit like over kill right now to do so.
export async function updateCacheGraph( deltaPayload ){
  try {
    const delta = await produceConceptSchemeDelta(deltaPayload);

    //always first delet then insert
    let delta = await produceConceptSchemeDelta(deltaPayload);
    //TODO: an optimisation step of folding the changesets
    // + removing redundant inserts/deletes (we have the cache graph we can compare to)
    // This will be a huge efficiency and ease of debugging win.
    // However, there is an annoying technical bit in folding, where sometimes
    // triples coming from deltas are in different lexical space and same logical value.
    // e.g. 2021-05-04T00:00:00Z vs 2021-05-04T00:00:000Z
    // Comparing these may not be super straightforward.
    // Because it's just bugprone to implement, need to look for the right library.
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
