import { uuid, sparqlEscapeUri } from 'mu';
import { STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         CACHE_GRAPH,
         INSERTION_CONTAINER,
         REMOVAL_CONTAINER
       } from '../env-config';
import {  updateTaskStatus, appendTaskError, appendGraphDatacontainerToTask } from '../lib/task';
import { sparqlEscapePredicate, batchedInsert } from '../lib/utils';

const EXPORT_CONFIG = require('/config/export.json');

export async function runHealingTask( task ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
    const removalContainer = await createDataGraphContainer(task, REMOVAL_CONTAINER);
    const insertionContainer = await createDataGraphContainer(task, INSERTION_CONTAINER);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;

    for( const config of EXPORT_CONFIG.export){
      //TODO: perhaps include this extra predicate in the config file
      const extendedProperties = [...config.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
      for(const property of extendedProperties){
        await calculateAndStoreTriplesToRemove(conceptSchemeUri,
                                               config,
                                               CACHE_GRAPH,
                                               config.type,
                                               property,
                                               removalContainer.graphUri);

        await calculateAndStoreTriplesToAdd(conceptSchemeUri,
                                            config,
                                            CACHE_GRAPH,
                                            config.type,
                                            property,
                                            insertionContainer.graphUri);
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

async function createDataGraphContainer(task, subject){
  const graphUri = `http://redpencil.data.gift/id/healing/graphs/${uuid()}`;
  const graphContainer = { id: uuid(), subject, graphUri };
  graphContainer.uri = `http://redpencil.data.gift/id/dataContainers/${graphContainer.id}`;
  await appendGraphDatacontainerToTask(task, graphContainer);
  return graphContainer;
}

async function calculateAndStoreTriplesToRemove(conceptSchemeUri, config, cacheGraph, type, property, graphContainer){
  const predicatePath = config.pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');

  const selectQuery = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      GRAPH ${sparqlEscapeUri(cacheGraph)}{
        ?subject a ${sparqlEscapeUri(type)}.
        ?subject ?predicate ?object.
      }

      FILTER NOT EXISTS {
        ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.
        GRAPH ?g {
          ?subject ?predicate ?object.
        }

        FILTER(?g NOT IN (${sparqlEscapeUri(cacheGraph)}))
      }
    }
    ORDER BY ?subject ?predicate ?object
  `;

  await batchedInsert(selectQuery, graphContainer); //Write to result container. keep order.
  await batchedInsert(selectQuery, cacheGraph);
}

async function calculateAndStoreTriplesToAdd(conceptSchemeUri, config, cacheGraph, type, property, graphContainer){
  const predicatePath = config.pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');

  const selectQuery = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      ?subject a ${sparqlEscapeUri(type)}.
      ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.

      ?subject ?predicate ?object.

      FILTER NOT EXISTS {
        GRAPH ${sparqlEscapeUri(cacheGraph)}{
          ?subject ?predicate ?object.
        }
      }
    }
    ORDER BY ?subject ?predicate ?object
  `;

  await batchedInsert(selectQuery, graphContainer); //Write to result container. keep order.
  await batchedInsert(selectQuery, cacheGraph);

}
