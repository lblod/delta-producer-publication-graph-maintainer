import { sparqlEscapeUri } from 'mu';
import { querySudo as query } from '@lblod/mu-auth-sudo';
import { INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION,
         HEALING_JOB_OPERATION,
         HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION,
         INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION,
         STATUS_SUCCESS,
         STATUS_BUSY,
         STATUS_SCHEDULED,
         TASK_TYPE,
         PREFIXES,
         JOB_TYPE
       } from '../env-config';
import { Delta } from '../lib/delta';

export async function doesDeltaContainNewTaskToProcess( deltaPayload ){
  const entries = new Delta(deltaPayload).getInsertsFor('http://www.w3.org/ns/adms#status', STATUS_SCHEDULED);
  let containsNewTask = false;

  for (let entry of entries) {
    if(await isNewTaskOfInterest( entry )){
      containsNewTask = true;
    }

  }

  return containsNewTask;
}

export async function hasInitialSyncRun(){
  const queryString = `
    ${PREFIXES}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(JOB_TYPE)};
             task:operation ${sparqlEscapeUri(INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)};
             adms:status ${sparqlEscapeUri(STATUS_SUCCESS)}.
      }
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

export async function isBlockingJobActive(){
  const queryString = `
    ${PREFIXES}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(JOB_TYPE)};
             task:operation ?operation;
             adms:status ?status.
      }
      FILTER( ?status IN (
        ${sparqlEscapeUri(STATUS_SCHEDULED)},
        ${sparqlEscapeUri(STATUS_BUSY)}
      ))
      FILTER( ?operation IN (
        ${sparqlEscapeUri(INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)},
        ${sparqlEscapeUri(HEALING_JOB_OPERATION)}
       )
      )
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

async function isNewTaskOfInterest( taskUri ){
  const queryString = `
    ${PREFIXES}

    SELECT DISTINCT ?job ?task WHERE {
      BIND(${sparqlEscapeUri(taskUri)} as ?task)
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(JOB_TYPE)};
            task:operation ?jobOperation.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(TASK_TYPE)};
            task:operation ?taskOperation;
            adms:status ${sparqlEscapeUri(STATUS_SCHEDULED)}.
       }
      FILTER( ?taskOperation IN (
         ${sparqlEscapeUri(INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION)},
         ${sparqlEscapeUri(HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION)}
      ))
      FILTER( ?jobOperation IN (
         ${sparqlEscapeUri(INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)},
         ${sparqlEscapeUri(HEALING_JOB_OPERATION)}
       )
      )
    }
  `;

  const result = await query(queryString);
  return result.results.bindings.length > 0;
}
