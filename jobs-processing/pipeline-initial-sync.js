import { sparqlEscapeUri } from 'mu';
import { CACHE_GRAPH,
         STATUS_FAILED,
         STATUS_SUCCESS,
         STATUS_BUSY
       } from '../env-config';
import { updateTaskStatus, appendTaskError } from '../lib/task';
import { sparqlEscapePredicate, batchedInsert } from '../lib/utils';

const EXPORT_CONFIG = require('/config/export.json');

export async function runInitialSyncTask( task ) {
  try {

    await updateTaskStatus(task, STATUS_BUSY);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;

    for( const config of EXPORT_CONFIG.export){
      //TODO: perhaps include this extra predicate in the config file
      const extendedProperties = [...config.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
      for(const property of extendedProperties){
        await feedCacheGraphWithConceptSchemeProperty(CACHE_GRAPH,
                                                      conceptSchemeUri,
                                                      config,
                                                      config.type,
                                                      property);
      }
    }

    await updateTaskStatus(task, STATUS_SUCCESS);

  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function feedCacheGraphWithConceptSchemeProperty(cacheGraph, conceptSchemeUri, config, type, property){
  const predicatePath = config.pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');

  const selectQuery = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      ?subject a ${sparqlEscapeUri(type)}.
      ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.

      ?subject ?predicate ?object.

    }
    ORDER BY ?subject ?predicate ?object
  `;

  await batchedInsert(selectQuery, cacheGraph);
}
