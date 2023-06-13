import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { Delta } from '../lib/delta';

export async function doesDeltaContainNewTaskToProcess(serviceConfig, deltaPayload ){
  const entries = new Delta(deltaPayload).getInsertsFor('http://www.w3.org/ns/adms#status', serviceConfig.statusScheduled);
  let containsNewTask = false;

  for (let entry of entries) {
    if(await isNewTaskOfInterest(serviceConfig, entry )){
      containsNewTask = true;
      break;
    }

  }

  return containsNewTask;
}

export async function hasInitialSyncRun(serviceConfig){
  const queryString = `
    ${serviceConfig.prefixes}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(serviceConfig.jobType)};
             task:operation ${sparqlEscapeUri(serviceConfig.initialPublicationGraphSyncJobOperation)};
             adms:status ${sparqlEscapeUri(serviceConfig.statusSuccess)}.
      }
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

export async function isBlockingJobActive(serviceConfig){
  const queryString = `
    ${serviceConfig.prefixes}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(serviceConfig.jobType)};
             task:operation ?operation;
             adms:status ?status.
      }
      FILTER( ?status IN (
        ${sparqlEscapeUri(serviceConfig.statusScheduled)},
        ${sparqlEscapeUri(serviceConfig.statusBusy)}
      ))
      FILTER( ?operation IN (
        ${sparqlEscapeUri(serviceConfig.initialPublicationGraphSyncJobOperation)},
        ${sparqlEscapeUri(serviceConfig.healingJobOperation)}
       )
      )
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

async function isNewTaskOfInterest(serviceConfig, taskUri ){
  const queryString = `
    ${serviceConfig.prefixes}

    SELECT DISTINCT ?job ?task WHERE {
      BIND(${sparqlEscapeUri(taskUri)} as ?task)
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(serviceConfig.jobType)};
            task:operation ?jobOperation.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(serviceConfig.taskType)};
            task:operation ?taskOperation;
            adms:status ${sparqlEscapeUri(serviceConfig.statusScheduled)}.
       }
      FILTER( ?taskOperation IN (
         ${sparqlEscapeUri(serviceConfig.initialPublicationGraphSyncTaskOperation)},
         ${sparqlEscapeUri(serviceConfig.healingPatchPublicationGraphTaskOperation)}
      ))
      FILTER( ?jobOperation IN (
         ${sparqlEscapeUri(serviceConfig.initialPublicationGraphSyncJobOperation)},
         ${sparqlEscapeUri(serviceConfig.healingJobOperation)}
       )
      )
    }
  `;

  const result = await query(queryString);
  return result.results.bindings.length > 0;
}

export function appendPublicationGraph(serviceConfig, tripleObject ){
  tripleObject.graph = {
    value: serviceConfig.publicationGraph,
    type: "uri"
  };
  return tripleObject;
}
