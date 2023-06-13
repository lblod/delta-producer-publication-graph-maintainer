import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { publishDeltaFiles } from '../../files-publisher/main';
import { appendTaskError, loadTask, updateTaskStatus } from '../../lib/task';
import { parseResult, storeError } from '../../lib/utils';
import { runHealingTask } from './pipeline-healing';

export async function executeHealingTask(serviceConfig, serviceExportConfig) {

  try {
    //TODO: extra checks are required to make sure the system remains in consistent state
    // (this responsibility might move to background scheduler)
    //so, e.g. only one scheduled task for jobs of interest, only one job of interest a time etc..
    const syncTaskUri = await getTaskUri(serviceConfig, serviceConfig.initialPublicationGraphSyncJobOperation, serviceConfig.initialPublicationGraphSyncTaskOperation, serviceConfig.statusScheduled);
    const healingTaskUri = await getTaskUri(serviceConfig, serviceConfig.healingJobOperation, serviceConfig.healingPatchPublicationGraphTaskOperation, serviceConfig.statusScheduled);

    let delta = { inserts: [], deletes: [] };

    if(syncTaskUri || healingTaskUri) {
      const task = await loadTask(serviceConfig, syncTaskUri || healingTaskUri);
      try {
        await updateTaskStatus(task, serviceConfig.statusBusy);
        delta = await runHealingTask(serviceConfig, serviceExportConfig, task, syncTaskUri ? true : false);
        if(serviceConfig.serveDeltaFiles && healingTaskUri){
          await publishDeltaFiles(serviceConfig, delta);
        }
        await updateTaskStatus(task, serviceConfig.statusSuccess);
      }
      catch(e) {
        await appendTaskError(serviceConfig, task, e.message || e);
        await updateTaskStatus(task, serviceConfig.statusFailed);
        throw e;
      }
    }

    return delta;
  }
  catch(e){
    console.error(e);
    await storeError(serviceConfig, e);
    throw e;
  }
}

//TODO: refactor this to a more generic task filter in lib/task.js
async function getTaskUri(serviceConfig, jobOperationUri, taskOperationUri, statusUri ){
  const queryString = `
    ${serviceConfig.prefixes}

    SELECT DISTINCT ?task WHERE {
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(serviceConfig.jobType)};
            task:operation ${sparqlEscapeUri(jobOperationUri)}.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(serviceConfig.taskType)};
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
