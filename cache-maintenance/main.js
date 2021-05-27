import { uuid, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { produceConceptSchemeDelta } from './producer';
import { CACHE_GRAPH } from '../env-config';
import { serializeTriple, storeError, batchedUpdate, batchedQuery } from '../lib/utils';
import { chain } from 'lodash';

//TODO: consider bringing the processing of cache under a job operation.
// It feels a bit like over kill right now to do so.
export async function updateCacheGraph( deltaPayload ){
  try {
    let delta = await produceConceptSchemeDelta(deltaPayload);
    const actualchanges = await filterActualChangesToCacheGraph(delta);

    const deletes = chain(actualchanges)
          .map(c => c.deletes)
          .flatten()
          .map(t => serializeTriple(t))
          .value();

    const inserts = chain(actualchanges)
          .map(c => c.inserts)
          .flatten()
          .map(t => serializeTriple(t))
          .value();

    if(deletes.length){
      await batchedUpdate(deletes, CACHE_GRAPH, 'DELETE');
    }

    if(inserts.length){
     await batchedUpdate(inserts, CACHE_GRAPH, 'INSERT');
    }

  }
  catch(error){
    const errorMsg = `Error while processing delta ${error}`;
    console.error(errorMsg);
    await storeError(errorMsg);
  }
}

async function filterActualChangesToCacheGraph(delta){
  // We need to fold the changeset to compare the effective wanted changes against the cache graph.
  // Suppose:
  //   - delta: [ deletes: s1, insert: s1]
  //   - targetGraph: { s1 }
  //   - An effective delete: remove a statement that was in the graph. Vice versa for effective insert.
  // Comparing the atomic delete from delta directly to the target graph, to conclude it is an effective change yields
  // wrong results, because the insert:s1 won't be considered an effective insert.
  // I.e. we end up with delta: [ deletes: s1 ] to execute against the cache graph.
  const foldedDelta = await foldChangeSet(delta);
  const foldedDeletes = chain(foldedDelta).map(c => c.deletes).flatten().value();
  const foldedInserts = chain(foldedDelta).map(c => c.inserts).flatten().value();
  const actualDeletes = [];

  //From this folded information, we now check wether the cache graph needs an update
  for(const triple of foldedDeletes){
    if((await tripleExists(triple, CACHE_GRAPH)) ){
      actualDeletes.push(triple);
      await new Promise(r => setTimeout(r, 1000)); //performance consideration
    }
  }

  const actualInserts = [];

  for(const triple of foldedInserts){
    if( !(await tripleExists(triple, CACHE_GRAPH)) ){
      actualInserts.push(triple);
      await new Promise(r => setTimeout(r, 1000));
    }
  }

  if(!(actualInserts.length || actualDeletes.length)){
    return [];
  }
  else {
    return [ { deletes: actualDeletes, inserts: actualInserts } ];
  }

}

async function tripleExists( tripleObject, graph ){
  const tripleStr = serializeTriple(tripleObject);
  const existsQuery = `
    ASK {
      GRAPH ${sparqlEscapeUri(graph)}{
        ${tripleStr}
      }
    }
  `;

  const result = await query(existsQuery);
  return result.boolean;
}

async function foldChangeSet( delta ){
  //Note: we don't use utils/diffNTriples because the lexical notation from deltas is not consistent
  // e.g. 2021-05-04T00:00:00Z vs 2021-05-04T00:00:000Z
  // Therefore, we use the database, that works in logical equivalents.
  // Implicit Assumption: we never expect huge (as in 50 000 triples) change sets at once.
  const deletes = chain(delta).map(c => c.deletes).flatten().value();
  const inserts = chain(delta).map(c => c.inserts).flatten().value();

  //Folding makes only sense when both deletes and inserts are there, else it is already folded
  if(!(deletes.length && inserts.length)){
    return delta;
  }

  else {
    const tempDeleteGraph = `http://mu.semte.ch/graphs/delta-producer-cache-maintainer/folding/deletes/${uuid()}`;
    const tempInsertGraph = `http://mu.semte.ch/graphs/delta-producer-cache-maintainer/folding/inserts/${uuid()}`;
    try {

      await batchedUpdate(deletes.map(t => serializeTriple(t)), tempDeleteGraph, 'INSERT');
      await batchedUpdate(inserts.map(t => serializeTriple(t)), tempInsertGraph, 'INSERT');

      const queryForFolding = ( sourceGraph, targetGraph ) => {
        return `
        SELECT DISTINCT ?subject ?predicate ?object WHERE {
            GRAPH ${sparqlEscapeUri(sourceGraph)}{
              ?subject ?predicate ?object.
            }
            FILTER NOT EXISTS {
            GRAPH ${sparqlEscapeUri(targetGraph)}{
              ?subject ?predicate ?object.
            }
           }
         }
        `;
      };

      const foldedDeletes = await batchedQuery(queryForFolding(tempDeleteGraph, tempInsertGraph), 1000);
      const foldedInserts = await batchedQuery(queryForFolding(tempInsertGraph, tempDeleteGraph), 1000);
      return [ { deletes: foldedDeletes, inserts: foldedInserts } ];
    }
    finally {

      const cleanUpQuery = (graph) => {
        return `
           DELETE {
               GRAPH ${sparqlEscapeUri(graph)} {
                 ?s ?p ?o
               }
           }
           WHERE {
               GRAPH ${sparqlEscapeUri(graph)} {
                 ?s ?p ?o
               }
           }
        `;
      };

      await update(cleanUpQuery(tempDeleteGraph));
      await update(cleanUpQuery(tempInsertGraph));
    }

  }

}
