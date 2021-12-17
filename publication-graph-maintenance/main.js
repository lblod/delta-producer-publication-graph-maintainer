import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { chain, chunk } from 'lodash';
import { uuid } from 'mu';
import {
  MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE,
  PUBLICATION_GRAPH, UPDATE_PUBLICATION_GRAPH_SLEEP, SKIP_MU_AUTH_DELTA_FOLDING,
  VIRTUOSO_ENDPOINT, MU_AUTH_ENDPOINT, HEALING_PATCH_GRAPH_BATCH_SIZE
} from '../env-config';
import { batchedQuery, batchedUpdate, compareTriple, parseResult, serializeTriple, storeError } from '../lib/utils';
import { produceDelta } from './producer';
import { sparqlEscapeUri } from 'mu';

//TODO: consider bringing the processing of publication under a job operation.
// It feels a bit like over kill right now to do so.
export async function updatePublicationGraph(deltaPayload) {
  try {
    let delta = await produceDelta(deltaPayload);
    //To remove unncessary deltas, we filter them out.
    const actualchanges = await filterActualChangesToPublicationGraph(delta);

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

    if (deletes.length) {
      await batchedUpdate(deletes,
          PUBLICATION_GRAPH,
          'DELETE',
          UPDATE_PUBLICATION_GRAPH_SLEEP,
          100,
          {'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE}
      );
    }

    if (inserts.length) {
      await batchedUpdate(inserts,
          PUBLICATION_GRAPH,
          'INSERT',
          UPDATE_PUBLICATION_GRAPH_SLEEP,
          100,
          {'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE}
      );
    }

  } catch (error) {
    const errorMsg = `Error while processing delta ${error}`;
    console.error(errorMsg);
    await storeError(errorMsg);
  }
}

async function filterActualChangesToPublicationGraph(delta) {
  // We need to fold the changeset to compare the effective wanted changes against the publication graph.
  // Suppose:
  //   - delta: [ deletes: s1, insert: s1]
  //   - targetGraph: { s1 }
  //   - An effective delete: remove a statement that was in the graph. Vice versa for effective insert.
  // Comparing the atomic delete from delta directly to the target graph, to conclude it is an effective change yields
  // wrong results, because the insert:s1 won't be considered an effective insert.
  // I.e. we end up with delta: [ deletes: s1 ] to execute against the publication graph.
  const dbEndpoint = SKIP_MU_AUTH_DELTA_FOLDING ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  const foldedDelta = await foldChangeSet(delta, {dbEndpoint});
  const foldedDeletes = chain(foldedDelta).map(c => c.deletes).flatten().value();
  const foldedInserts = chain(foldedDelta).map(c => c.inserts).flatten().value();

  const actualDeletes = await getActualDeletes(foldedDeletes, {dbEndpoint});
  const actualInserts = await getActualInserts(foldedInserts, {dbEndpoint});

  if (!(actualInserts.length || actualDeletes.length)) {
    return [];
  } else {
    return [{deletes: actualDeletes, inserts: actualInserts}];
  }

}

async function getActualDeletes(deletes, {dbEndpoint}) {
  return await findMatchesInStore(deletes, {
    graph: PUBLICATION_GRAPH,
    dbEndpoint
  });
}

async function getActualInserts(inserts, {dbEndpoint}) {
  const inPublicationGraph = await findMatchesInStore(inserts, {
    graph: PUBLICATION_GRAPH,
    dbEndpoint
  });
  return inserts.filter(t1 => !inPublicationGraph.find(t2 => compareTriple(t1, t2)));
}

async function findMatchesInStore(candidates, {
  graph,
  dbEndpoint,
  batchSize = HEALING_PATCH_GRAPH_BATCH_SIZE
} = {}) {
  const generateQuery = (candidates, graph) => `SELECT DISTINCT ?subject ?predicate ?object 
WHERE { 
  VALUES (?subject ?predicate ?object) { 
  ${candidates.map(triple => `(${serializeTriple(triple, {close: false})})`).join('\n')} 
  }
  GRAPH ${sparqlEscapeUri(graph)} {
    ?subject ?predicate ?object .
  } 
}`;

  const queries =
      chunk(candidates, batchSize)
          .map(candidates => generateQuery(candidates, graph));

  let matches = [];
  for (let q of queries) {
    matches = matches.concat(parseResult(await query(q, {}, dbEndpoint), {flatten: false}));
    await new Promise(r => setTimeout(r, UPDATE_PUBLICATION_GRAPH_SLEEP)); //performance consideration
  }
  return matches;
}

async function foldChangeSet(delta, config) {
  //Note: we don't use utils/diffNTriples because the lexical notation from deltas is not consistent
  // e.g. 2021-05-04T00:00:00Z vs 2021-05-04T00:00:000Z
  // Therefore, we use the database, that works in logical equivalents.
  // Implicit Assumption: we never expect huge (as in 50 000 triples) change sets at once.
  const deletes = chain(delta).map(c => c.deletes).flatten().value();
  const inserts = chain(delta).map(c => c.inserts).flatten().value();

  const tempDeleteGraph = `http://mu.semte.ch/graphs/delta-producer-publication-maintainer/folding/deletes/${uuid()}`;
  const tempInsertGraph = `http://mu.semte.ch/graphs/delta-producer-publication-maintainer/folding/inserts/${uuid()}`;
  try {

    await batchedUpdate(deletes.map(t => serializeTriple(t)),
        tempDeleteGraph,
        'INSERT',
        UPDATE_PUBLICATION_GRAPH_SLEEP,
        100,
        {'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE},
        config.dbEndpoint
    );
    await batchedUpdate(inserts.map(t => serializeTriple(t)),
        tempInsertGraph,
        'INSERT',
        UPDATE_PUBLICATION_GRAPH_SLEEP,
        100,
        {'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE},
        config.dbEndpoint
    );

    const queryForFolding = (sourceGraph, targetGraph) => {
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

    const foldedDeletes = await batchedQuery(queryForFolding(tempDeleteGraph, tempInsertGraph), 1000,
        config.dbEndpoint);
    const foldedInserts = await batchedQuery(queryForFolding(tempInsertGraph, tempDeleteGraph), 1000,
        config.dbEndpoint);
    return [{deletes: foldedDeletes, inserts: foldedInserts}];
  } finally {

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

    await update(cleanUpQuery(tempDeleteGraph), {'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE},
        config.dbEnpoint);
    await update(cleanUpQuery(tempInsertGraph), {'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE},
        config.dbEnpoint);
  }
}
