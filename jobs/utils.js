import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { Delta } from '../lib/delta';

export async function doesDeltaContainNewTaskToProcess( _config, deltaPayload ){
  const entries = new Delta(deltaPayload).getInsertsFor('http://www.w3.org/ns/adms#status', _config.STATUS_SCHEDULED);
  let containsNewTask = false;

  for (let entry of entries) {
    if(await isNewTaskOfInterest( _config, entry )){
      containsNewTask = true;
    }

  }

  return containsNewTask;
}

export async function hasInitialSyncRun(_config){
  const queryString = `
    ${_config.PREFIXES}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(_config.JOB_TYPE)};
             task:operation ${sparqlEscapeUri(_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)};
             adms:status ${sparqlEscapeUri(_config.STATUS_SUCCESS)}.
      }
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

export async function isBlockingJobActive(_config){
  const queryString = `
    ${_config.PREFIXES}
    SELECT DISTINCT ?job WHERE {
      GRAPH ?g {
        ?job a ${sparqlEscapeUri(_config.JOB_TYPE)};
             task:operation ?operation;
             adms:status ?status.
      }
      FILTER( ?status IN (
        ${sparqlEscapeUri(_config.STATUS_SCHEDULED)},
        ${sparqlEscapeUri(_config.STATUS_BUSY)}
      ))
      FILTER( ?operation IN (
        ${sparqlEscapeUri(_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)},
        ${sparqlEscapeUri(_config.HEALING_JOB_OPERATION)}
       )
      )
    }
  `;
  const result = await query(queryString);
  return result.results.bindings.length;
}

async function isNewTaskOfInterest( _config, taskUri ){
  const queryString = `
    ${_config.PREFIXES}

    SELECT DISTINCT ?job ?task WHERE {
      BIND(${sparqlEscapeUri(taskUri)} as ?task)
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(_config.JOB_TYPE)};
            task:operation ?jobOperation.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(_config.TASK_TYPE)};
            task:operation ?taskOperation;
            adms:status ${sparqlEscapeUri(_config.STATUS_SCHEDULED)}.
       }
      FILTER( ?taskOperation IN (
         ${sparqlEscapeUri(_config.INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION)},
         ${sparqlEscapeUri(_config.HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION)}
      ))
      FILTER( ?jobOperation IN (
         ${sparqlEscapeUri(_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)},
         ${sparqlEscapeUri(_config.HEALING_JOB_OPERATION)}
       )
      )
    }
  `;

  const result = await query(queryString);
  return result.results.bindings.length > 0;
}

export function appendPublicationGraph( _config, tripleObject ){
  tripleObject.graph = {
    value: _config.PUBLICATION_GRAPH,
    type: "uri"
  };
  return tripleObject;
}
