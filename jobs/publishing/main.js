import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { chain } from 'lodash';
import { uuid } from 'mu';
import { batchedQuery, batchedUpdate, serializeTriple, storeError } from '../../lib/utils';
import { produceDelta } from './producer';
import { sparqlEscapeUri } from 'mu';
import { appendPublicationGraph } from '../utils';

//TODO: consider bringing the processing of publication under a job operation.
// It feels a bit like over kill right now to do so.
export async function updatePublicationGraph( _config, _export_config, deltaPayload ){
  try {
    let delta = await produceDelta(_config, _export_config, deltaPayload);
    //To remove unnecessary deltas, we filter them out.
    const actualchanges = await filterActualChangesToPublicationGraph(_config, delta);

    const deletes = chain(actualchanges)
          .map(c => c.deletes)
          .flatten()
          .value();

    const inserts = chain(actualchanges)
          .map(c => c.inserts)
          .flatten()
          .value();

    if(deletes.length){
      await batchedUpdate(_config, deletes.map(_config, t => serializeTriple(t)),
                          _config.PUBLICATION_GRAPH,
                          'DELETE',
                          _config.UPDATE_PUBLICATION_GRAPH_SLEEP,
                          100,
                          { 'mu-call-scope-id':  _config.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE },
                          _config.PUBLICATION_MU_AUTH_ENDPOINT
                         );
    }

    if(inserts.length){
      await batchedUpdate(_config, inserts.map(t => serializeTriple(t)),
                          _config.PUBLICATION_GRAPH,
                          'INSERT',
                          _config.UPDATE_PUBLICATION_GRAPH_SLEEP,
                          100,
                          { 'mu-call-scope-id':  _config.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE },
                          _config.PUBLICATION_MU_AUTH_ENDPOINT
                         );
    }

    return {
      inserts: inserts.map(t => appendPublicationGraph(_config, t)),
      deletes: deletes.map(t => appendPublicationGraph(_config, t))
    };
  }
  catch(error){
    const errorMsg = `Error while processing delta ${error}`;
    console.error(errorMsg);
    await storeError(_config, errorMsg);
    throw error;
  }
}

async function filterActualChangesToPublicationGraph(_config, delta){
  // We need to fold the changeset to compare the effective wanted changes against the publication graph.
  // Suppose:
  //   - delta: [ deletes: s1, insert: s1]
  //   - targetGraph: { s1 }
  //   - An effective delete: remove a statement that was in the graph. Vice versa for effective insert.
  // Comparing the atomic delete from delta directly to the target graph, to conclude it is an effective change yields
  // wrong results, because the insert:s1 won't be considered an effective insert.
  // I.e. we end up with delta: [ deletes: s1 ] to execute against the publication graph.
  const dbEndpoint = _config.SKIP_MU_AUTH_DELTA_FOLDING ? _config.PUBLICATION_VIRTUOSO_ENDPOINT : _config.PUBLICATION_MU_AUTH_ENDPOINT;
  const foldedDelta = await foldChangeSet(_config, delta, { dbEndpoint });
  const foldedDeletes = chain(foldedDelta).map(c => c.deletes).flatten().value();
  const foldedInserts = chain(foldedDelta).map(c => c.inserts).flatten().value();
  const actualDeletes = [];

  //From this folded information, we now check whether the publication graph needs an update
  for(const triple of foldedDeletes){
    if((await tripleExists(triple, _config.PUBLICATION_GRAPH, { dbEndpoint })) ){
      actualDeletes.push(triple);
      await new Promise(r => setTimeout(r, _config.UPDATE_PUBLICATION_GRAPH_SLEEP)); //performance consideration
    }
  }

  const actualInserts = [];

  for(const triple of foldedInserts){
    if( !(await tripleExists(triple, _config.PUBLICATION_GRAPH, { dbEndpoint })) ){
      actualInserts.push(triple);
      await new Promise(r => setTimeout(r, _config.UPDATE_PUBLICATION_GRAPH_SLEEP));
    }
  }

  if(!(actualInserts.length || actualDeletes.length)){
    return [];
  }
  else {
    return [ { deletes: actualDeletes, inserts: actualInserts } ];
  }

}

async function tripleExists( tripleObject, graph, config ){
  const tripleStr = serializeTriple(tripleObject);
  const existsQuery = `
    ASK {
      GRAPH ${sparqlEscapeUri(graph)}{
        ${tripleStr}
      }
    }
  `;

  const result = await query(existsQuery, {}, { sparqlEndpoint: config.dbEndpoint, mayRetry: true } );
  return result.boolean;
}

async function foldChangeSet( _config, delta, config ){
  //Note: we don't use utils/diffNTriples because the lexical notation from deltas is not consistent
  // e.g. 2021-05-04T00:00:00Z vs 2021-05-04T00:00:000Z
  // Therefore, we use the database, that works in logical equivalents.
  // Implicit Assumption: we never expect huge (as in 50 000 triples) change sets at once.
  const deletes = chain(delta).map(c => c.deletes).flatten().value();
  const inserts = chain(delta).map(c => c.inserts).flatten().value();

  const tempDeleteGraph = `http://mu.semte.ch/graphs/delta-producer-publication-maintainer/folding/deletes/${uuid()}`;
  const tempInsertGraph = `http://mu.semte.ch/graphs/delta-producer-publication-maintainer/folding/inserts/${uuid()}`;
  try {

    await batchedUpdate(_config, deletes.map(t => serializeTriple(t)),
                        tempDeleteGraph,
                        'INSERT',
                        _config.UPDATE_PUBLICATION_GRAPH_SLEEP,
                        100,
                        { 'mu-call-scope-id':  _config.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE },
                        config.dbEndpoint
                       );
    await batchedUpdate(_config, inserts.map(t => serializeTriple(t)),
                        tempInsertGraph,
                        'INSERT',
                        _config.UPDATE_PUBLICATION_GRAPH_SLEEP,
                        100,
                        { 'mu-call-scope-id':  _config.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE },
                        config.dbEndpoint
                       );

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

    const foldedDeletes = await batchedQuery(_config, queryForFolding(tempDeleteGraph, tempInsertGraph), 1000, config.dbEndpoint);
    const foldedInserts = await batchedQuery(_config, queryForFolding(tempInsertGraph, tempDeleteGraph), 1000, config.dbEndpoint);
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

    await update(cleanUpQuery(tempDeleteGraph), { 'mu-call-scope-id':  _config.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE },
                 { sparqlEndpoint: config.dbEndpoint, mayRetry: true }
                );
    await update(cleanUpQuery(tempInsertGraph), { 'mu-call-scope-id':  _config.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE },
                 { sparqlEndpoint: config.dbEndpoint, mayRetry: true }
                );
  }
}
