import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { appendTaskError, loadTask, updateTaskStatus } from '../../lib/task';
import { parseResult, storeError } from '../../lib/utils';
import { runHealingTask } from './pipeline-healing';

export async function executeHealingTask(service_config, service_export_config) {

  try {
    //TODO: extra checks are required to make sure the system remains in consistent state
    // (this responsibility might move to background scheduler)
    //so, e.g. only one scheduled task for jobs of interest, only one job of interest a time etc..
    const syncTaskUri = await getTaskUri(service_config, service_config.initialPublicationGraphSyncJobOperation, service_config.initialPublicationGraphSyncTaskOperation, service_config.statusScheduled);
    const healingTaskUri = await getTaskUri(service_config, service_config.healingJobOperation, service_config.healingPatchPublicationGraphTaskOperation, service_config.statusScheduled);

    let delta = { inserts: [], deletes: [] };

    if(syncTaskUri || healingTaskUri) {
      const task = await loadTask(service_config, syncTaskUri || healingTaskUri);
      try {
        await updateTaskStatus(task, service_config.statusBusy);
        await runHealingTask(service_config, service_export_config, task, syncTaskUri ? true : false, service_config.serveDeltaFiles && healingTaskUri);
        await updateTaskStatus(task, service_config.statusSuccess);
      }
      catch(e) {
        await appendTaskError(service_config, task, e.message || e);
        await updateTaskStatus(task, service_config.statusFailed);
        throw e;
      }
    }

    return delta;
  }
  catch(e){
    console.error(e);
    await storeError(service_config, e);
    throw e;
  }
}

//TODO: refactor this to a more generic task filter in lib/task.js
async function getTaskUri(service_config, jobOperationUri, taskOperationUri, statusUri ){
  const queryString = `
    ${service_config.prefixes}

    SELECT DISTINCT ?task WHERE {
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(service_config.jobType)};
            task:operation ${sparqlEscapeUri(jobOperationUri)}.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(service_config.taskType)};
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
