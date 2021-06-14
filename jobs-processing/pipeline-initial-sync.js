import { sparqlEscapeUri, sparqlEscapeString } from 'mu';
import { PUBLICATION_GRAPH,
         STATUS_FAILED,
         STATUS_SUCCESS,
         STATUS_BUSY,
         MU_CALL_SCOPE_ID_INITIAL_SYNC,
         USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS,
         VIRTUOSO_ENDPOINT,
         MU_AUTH_ENDPOINT
       } from '../env-config';
import { updateTaskStatus, appendTaskError } from '../lib/task';
import { sparqlEscapePredicate, batchedQuery, batchedUpdate, serializeTriple } from '../lib/utils';

const EXPORT_CONFIG = require('/config/export.json');

export async function runInitialSyncTask( task ) {
  try {

    await updateTaskStatus(task, STATUS_BUSY);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;

    for( const config of EXPORT_CONFIG.export){
      //TODO: perhaps include this extra predicate in the config file
      const extendedProperties = [...config.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
      for(const property of extendedProperties){
        await feedPublicationGraphWithConceptSchemeProperty(PUBLICATION_GRAPH,
                                                      conceptSchemeUri,
                                                      config,
                                                      config.type,
                                                      property,
                                                      config.graphsFilter || []);
      }
    }

    await updateTaskStatus(task, STATUS_SUCCESS);

  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message || e);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function feedPublicationGraphWithConceptSchemeProperty(publicationGraph, conceptSchemeUri, config, type, property, graphsFilter){
  const predicatePath = config.pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');

  let selectQuery = '';

  if(graphsFilter.length) {

    const graphsFilterStr = graphsFilter
          .map(g => `regex(str(?g), ${sparqlEscapeString(g)})`)
          .join(' || ');

    selectQuery = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)

        ?subject a ${sparqlEscapeUri(type)}.
        ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.

        GRAPH ?g {
          ?subject ?predicate ?object.
        }
        FILTER ( ${graphsFilterStr} )
        FILTER NOT EXISTS {
          GRAPH ${sparqlEscapeUri(publicationGraph)}{
            ?subject ?predicate ?object.
          }
        }
      }
    `;

  }
  else {

    selectQuery = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)

        ?subject a ${sparqlEscapeUri(type)}.
        ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.
        ?subject ?predicate ?object.

        FILTER NOT EXISTS {
          GRAPH ${sparqlEscapeUri(publicationGraph)}{
            ?subject ?predicate ?object.
          }
        }
      }
    `;

  }

  const sourceResult = await batchedQuery(selectQuery,
                                          1000,
                                          USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT);
  const sourceNTriples = sourceResult.map(t => serializeTriple(t));
  await batchedUpdate(sourceNTriples,
                      publicationGraph,
                      'INSERT',
                      500,
                      100,
                      { 'mu-call-scope-id': MU_CALL_SCOPE_ID_INITIAL_SYNC }
                     );
}
