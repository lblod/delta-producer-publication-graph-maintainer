import { querySudo as query } from '@lblod/mu-auth-sudo';
import { sparqlEscapeUri } from 'mu';
import { publishDeltaFiles } from '../../files-publisher/main';
import { appendTaskError, loadTask, updateTaskStatus } from '../../lib/task';
import { parseResult, storeError } from '../../lib/utils';
import { runHealingTask } from './pipeline-healing';

export async function executeHealingTask(_config){

  try {
    //TODO: extra checks are required to make sure the system remains in consistent state
    // (this responsibility might move to background scheduler)
    //so, e.g. only one scheduled task for jobs of interest, only one job of interest a time etc..
    const syncTaskUri = await getTaskUri(_config, _config.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION, _config.INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION, _config.STATUS_SCHEDULED);
    const healingTaskUri = await getTaskUri(_config, _config.HEALING_JOB_OPERATION, _config.HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION, _config.STATUS_SCHEDULED);

    let delta = { inserts: [], deletes: [] };

    if(syncTaskUri || healingTaskUri) {
      const task = await loadTask(_config, syncTaskUri || healingTaskUri);
      try {
        await updateTaskStatus(task, _config.STATUS_BUSY);
        delta = await runHealingTask(_config, task, syncTaskUri ? true : false);
        if(_config.SERVE_DELTA_FILES && healingTaskUri){
          await publishDeltaFiles(_config, delta);
        }
        await updateTaskStatus(task, _config.STATUS_SUCCESS);
      }
      catch(e) {
        await appendTaskError(_config, task, e.message || e);
        await updateTaskStatus(task, _config.STATUS_FAILED);
        throw e;
      }
    }

    return delta;
  }
  catch(e){
    console.error(e);
    await storeError(_config, e);
    throw e;
  }
}

//TODO: refactor this to a more generic task filter in lib/task.js
async function getTaskUri( _config, jobOperationUri, taskOperationUri, statusUri ){
  const queryString = `
    ${_config.PREFIXES}

    SELECT DISTINCT ?task WHERE {
      GRAPH ?g {
          ?job a ${sparqlEscapeUri(_config.JOB_TYPE)};
            task:operation ${sparqlEscapeUri(jobOperationUri)}.

          ?task dct:isPartOf ?job;
            a ${sparqlEscapeUri(_config.TASK_TYPE)};
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
