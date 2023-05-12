import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { Delta } from '../lib/delta';

export async function doesDeltaContainNewTaskToProcess(service_config, deltaPayload ){
  const entries = new Delta(deltaPayload).getInsertsFor('http://www.w3.org/ns/adms#status', service_config.statusScheduled);
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
    ${service_config.prefixes}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(service_config.jobType)};
             task:operation ${sparqlEscapeUri(service_config.initialPublicationGraphSyncJobOperation)};
             adms:status ${sparqlEscapeUri(service_config.statusSuccess)}.
      }
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

export async function isBlockingJobActive(service_config){
  const queryString = `
    ${service_config.prefixes}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(service_config.jobType)};
             task:operation ?operation;
             adms:status ?status.
      }
      FILTER( ?status IN (
        ${sparqlEscapeUri(service_config.statusScheduled)},
        ${sparqlEscapeUri(service_config.statusBusy)}
      ))
      FILTER( ?operation IN (
        ${sparqlEscapeUri(service_config.initialPublicationGraphSyncJobOperation)},
        ${sparqlEscapeUri(service_config.healingJobOperation)}
       )
      )
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

async function isNewTaskOfInterest(service_config, taskUri ){
  const queryString = `
    ${service_config.prefixes}

    SELECT DISTINCT ?job ?task WHERE {
      BIND(${sparqlEscapeUri(taskUri)} as ?task)
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(service_config.jobType)};
            task:operation ?jobOperation.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(service_config.taskType)};
            task:operation ?taskOperation;
            adms:status ${sparqlEscapeUri(service_config.statusScheduled)}.
       }
      FILTER( ?taskOperation IN (
         ${sparqlEscapeUri(service_config.initialPublicationGraphSyncTaskOperation)},
         ${sparqlEscapeUri(service_config.healingPatchPublicationGraphTaskOperation)}
      ))
      FILTER( ?jobOperation IN (
         ${sparqlEscapeUri(service_config.initialPublicationGraphSyncJobOperation)},
         ${sparqlEscapeUri(service_config.healingJobOperation)}
       )
      )
    }
  `;

  const result = await query(queryString);
  return result.results.bindings.length > 0;
}

export function appendPublicationGraph(service_config, tripleObject ){
  tripleObject.graph = {
    value: service_config.publicationGraph,
    type: "uri"
  };
  return tripleObject;
}
