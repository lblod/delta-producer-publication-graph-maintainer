import { sparqlEscapeUri } from 'mu';
import { querySudo as query } from '@lblod/mu-auth-sudo';
import { INITIAL_CACHE_SYNC_JOB_OPERATION,
         INITIAL_CACHE_SYNC_TASK_OPERATION,
         HEALING_JOB_OPERATION,
         HEALING_PATCH_CACHE_GRAPH_TASK_OPERATION,
         STATUS_SCHEDULED,
         TASK_TYPE,
         PREFIXES,
         JOB_TYPE
       } from '../env-config';

import { loadTask } from '../lib/task';
import { runInitialSyncTask } from './pipeline-initial-sync';
import { runHealingTask } from './pipeline-healing';
import { parseResult } from '../lib/utils';

export async function executeScheduledTask(){
  //TODO: extra checks are required to make sure the system remains in consistent state
  //so, e.g. only one scheduled task for jobs of interest, only one job of intersest a time etc..
  const syncTaskUri = await getTaskUri(INITIAL_CACHE_SYNC_JOB_OPERATION, INITIAL_CACHE_SYNC_TASK_OPERATION, STATUS_SCHEDULED);
  const healingTaskUri = await getTaskUri(HEALING_JOB_OPERATION, HEALING_PATCH_CACHE_GRAPH_TASK_OPERATION, STATUS_SCHEDULED);

  if(syncTaskUri){
    const task = await loadTask(syncTaskUri);
    runInitialSyncTask(task);
  }
  else if(healingTaskUri){
    const task = await loadTask(syncTaskUri);
    runHealingTask(task);
  }
}

//TODO: refactor this to a more generic task filter in lib/task.js
async function getTaskUri( jobOperationUri, taskOperationUri, statusUri ){
  const queryString = `
    ${PREFIXES}

    SELECT DISTINCT ?task WHERE {
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(JOB_TYPE)};
            task:operation ${sparqlEscapeUri(jobOperationUri)}.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(TASK_TYPE)};
            task:operation ${sparqlEscapeUri(taskOperationUri)};
            adms:status ${sparqlEscapeUri(statusUri)}.
       }
    }
  `;

  const results = parseResult(await query(queryString));

  if(results.length){
    return results[0].task;
  }
  else return null;
}
