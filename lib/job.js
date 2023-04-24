import { sparqlEscapeString, sparqlEscapeDateTime, sparqlEscapeUri } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { parseResult } from './utils';
import { createError, removeError } from './error';

export async function loadJob( _config, subject ){
  const queryJob = `
    ${_config.PREFIXES}
    SELECT DISTINCT ?graph ?job ?created ?modified ?creator ?status ?error ?operation WHERE {
     GRAPH ?graph {
       BIND(${sparqlEscapeUri(subject)} AS ?job)
       ?job a ${sparqlEscapeUri(_config.JOB_TYPE)};
         dct:creator ?creator;
         adms:status ?status;
         dct:created ?created;
         task:operation ?operation;
         dct:modified ?modified.

       OPTIONAL { ?job task:error ?error. }
     }
    }
  `;

  const job = parseResult(await query(queryJob))[0];
  if(!job) return null;

  //load has many
  const queryTasks = `
   ${_config.PREFIXES}
   SELECT DISTINCT ?job ?task WHERE {
     GRAPH ?g {
       BIND(${ sparqlEscapeUri(subject) } as ?job)
       ?task dct:isPartOf ?job
      }
    }
  `;

  const tasks = parseResult(await query(queryTasks)).map(row => row.task);
  job.tasks = tasks;

  return job;
}

export async function updateJob( _config, job ){
  const storedJobData = await loadJob(job.job);
  if(storedJobData.error){
    await removeError(storedJobData.error); //TODO: fix ambuigity. Loading loads URI for relation. Update expects hash
  }

  job.modified = new Date();

  const tasksTriples = job.tasks
        .map(task => `${sparqlEscapeUri(task)} dct:isPartOf ${sparqlEscapeUri(job.job)}.`)
        .join('\n');

  let errorTriple = '';
  if(job.error){
    const error = await createError(job.graph, job.error.message);
    errorTriple = `${sparqlEscapeUri(job.job)} task:error ${sparqlEscapeString(error.error)}.`;
  }

  const updateQuery = `
    ${_config.PREFIXES}
    DELETE {
      GRAPH ?g {
        ?job dct:creator ?creator;
           adms:status ?status;
           dct:created ?created;
           task:operation ?operation;
           dct:modified ?modified.

        ?task dct:isPartOf ?job.
        ?job task:error ?error.
      }
    }
    WHERE {
      GRAPH ?g {
       BIND(${sparqlEscapeUri(job.job)} AS ?job)
       ?job a ${sparqlEscapeUri(_config.JOB_TYPE)};
         dct:creator ?creator;
         adms:status ?status;
         dct:created ?created;
         task:operation ?operation;
         dct:modified ?modified.

       OPTIONAL { ?task dct:isPartOf ?job. }
       OPTIONAL { ?job task:error ?error. }
      }
    }

    ;

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(job.graph)}{
        ${sparqlEscapeUri(job.job)} dct:creator ${sparqlEscapeUri(job.creator)};
           adms:status ${sparqlEscapeUri(job.status)};
           dct:created ${sparqlEscapeDateTime(job.created)};
           task:operation ${sparqlEscapeUri(job.operation)};
           dct:modified ${sparqlEscapeDateTime(job.modified)}.

        ${errorTriple}
        ${tasksTriples}
      }
    }
  `;

  await update(updateQuery);

  return loadJob(job.job);
}
