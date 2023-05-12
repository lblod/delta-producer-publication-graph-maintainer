import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { publishDeltaFiles } from '../../files-publisher/main';
import { appendTaskError, loadTask, updateTaskStatus } from '../../lib/task';
import { parseResult, storeError } from '../../lib/utils';
import { runHealingTask } from './pipeline-healing';

export async function executeHealingTask(service_config, service_export_config) {

  try {
    //TODO: extra checks are required to make sure the system remains in consistent state
    // (this responsibility might move to background scheduler)
    //so, e.g. only one scheduled task for jobs of interest, only one job of interest a time etc..
    const syncTaskUri = await getTaskUri(service_config, service_config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION, service_config.INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION, service_config.STATUS_SCHEDULED);
    const healingTaskUri = await getTaskUri(service_config, service_config.HEALING_JOB_OPERATION, service_config.HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION, service_config.STATUS_SCHEDULED);

    let delta = { inserts: [], deletes: [] };

    if(syncTaskUri || healingTaskUri) {
      const task = await loadTask(service_config, syncTaskUri || healingTaskUri);
      try {
        await updateTaskStatus(task, service_config.STATUS_BUSY);
        delta = await runHealingTask(service_config, service_export_config, task, syncTaskUri ? true : false);
        if(service_config.SERVE_DELTA_FILES && healingTaskUri){
          await publishDeltaFiles(service_config, delta);
        }
        await updateTaskStatus(task, service_config.STATUS_SUCCESS);
      }
      catch(e) {
        await appendTaskError(service_config, task, e.message || e);
        await updateTaskStatus(task, service_config.STATUS_FAILED);
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
    ${service_config.PREFIXES}

    SELECT DISTINCT ?task WHERE {
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(service_config.JOB_TYPE)};
            task:operation ${sparqlEscapeUri(jobOperationUri)}.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(service_config.TASK_TYPE)};
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
