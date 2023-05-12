import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { Delta } from '../lib/delta';

export async function doesDeltaContainNewTaskToProcess(service_config, deltaPayload ){
  const entries = new Delta(deltaPayload).getInsertsFor('http://www.w3.org/ns/adms#status', service_config.STATUS_SCHEDULED);
  let containsNewTask = false;

  for (let entry of entries) {
    if(await isNewTaskOfInterest(service_config, entry )){
      containsNewTask = true;
    }

  }

  return containsNewTask;
}

export async function hasInitialSyncRun(service_config){
  const queryString = `
    ${service_config.PREFIXES}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(service_config.JOB_TYPE)};
             task:operation ${sparqlEscapeUri(service_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)};
             adms:status ${sparqlEscapeUri(service_config.STATUS_SUCCESS)}.
      }
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

export async function isBlockingJobActive(service_config){
  const queryString = `
    ${service_config.PREFIXES}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(service_config.JOB_TYPE)};
             task:operation ?operation;
             adms:status ?status.
      }
      FILTER( ?status IN (
        ${sparqlEscapeUri(service_config.STATUS_SCHEDULED)},
        ${sparqlEscapeUri(service_config.STATUS_BUSY)}
      ))
      FILTER( ?operation IN (
        ${sparqlEscapeUri(service_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)},
        ${sparqlEscapeUri(service_config.HEALING_JOB_OPERATION)}
       )
      )
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

async function isNewTaskOfInterest(service_config, taskUri ){
  const queryString = `
    ${service_config.PREFIXES}

    SELECT DISTINCT ?job ?task WHERE {
      BIND(${sparqlEscapeUri(taskUri)} as ?task)
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(service_config.JOB_TYPE)};
            task:operation ?jobOperation.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(service_config.TASK_TYPE)};
            task:operation ?taskOperation;
            adms:status ${sparqlEscapeUri(service_config.STATUS_SCHEDULED)}.
       }
      FILTER( ?taskOperation IN (
         ${sparqlEscapeUri(service_config.INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION)},
         ${sparqlEscapeUri(service_config.HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION)}
      ))
      FILTER( ?jobOperation IN (
         ${sparqlEscapeUri(service_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)},
         ${sparqlEscapeUri(service_config.HEALING_JOB_OPERATION)}
       )
      )
    }
  `;

  const result = await query(queryString);
  return result.results.bindings.length > 0;
}

export function appendPublicationGraph(service_config, tripleObject ){
  tripleObject.graph = {
    value: service_config.PUBLICATION_GRAPH,
    type: "uri"
  };
  return tripleObject;
}
