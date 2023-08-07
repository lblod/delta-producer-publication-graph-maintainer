import { uniq } from 'lodash';
import { appendTaskResultFile } from '../../lib/task';
import { writeTtlFile } from '../../lib/file-helpers';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { batchedUpdate, batchedQuery, sparqlEscapePredicate, serializeTriple } from '../../lib/utils';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { rmSync } from 'fs';
const SHARE_FOLDER = '/share/';

import {
  MU_AUTH_ENDPOINT,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
  VIRTUOSO_ENDPOINT
} from "../../env-config";

export function groupPathToConceptSchemePerProperty(config){
  const result = {};
  for( const configEntry of config){
    //TODO: perhaps include this extra predicate in the config file
    let extendedProperties = [...configEntry.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
    extendedProperties = uniq(extendedProperties); //TODO: perhaps crash instead of being the silent fixer
    for(const property of extendedProperties){
      if(result[property]){
        result[property].push(configEntry);
      }
      else {
        result[property] = [ configEntry ];
      }

    }
  }
  return result;
}

export async function createResultsContainer(serviceConfig, task, nTriples, subject, fileName ){
  const fileContainer = { id: uuid(), subject };
  fileContainer.uri = `http://data.lblod.info/id/dataContainers/${fileContainer.id}`;
  const turtleFile = await writeTtlFile(serviceConfig.reportingFilesGraph || task.graph, nTriples.join('\n'), fileName);
  await appendTaskResultFile(task, fileContainer, turtleFile);
}

/*
 * Gets the triples residing in the publication graph, for a specific property
 */
export async function getScopedPublicationTriples(serviceConfig, config, property, publicationGraph){
  const { type, healingOptions } = config;

  const healingOptionsForProperty = healingOptions && healingOptions[property] ?
      healingOptions[property] : { 'queryChunkSize': 0 };

  console.log(`Publication triples using file? ${serviceConfig.useFileDiff}`);
  const endpoint = serviceConfig.useVirtuosoForExpensiveSelects ? PUBLICATION_VIRTUOSO_ENDPOINT : PUBLICATION_MU_AUTH_ENDPOINT;

  const selectFromPublicationGraph = `
   SELECT DISTINCT ?subject ?predicate ?object WHERE {
    GRAPH ${sparqlEscapeUri(publicationGraph)}{
      BIND(${sparqlEscapeUri(property)} as ?predicate)
      ?subject a ${sparqlEscapeUri(type)}.
      ?subject ?predicate ?object.
    }
   }
  `;

  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await batchedQuery(selectFromPublicationGraph,
                                    healingOptionsForProperty.queryChunkSize,
                                    endpoint
                                   );

  return reformatQueryResult(result, property);
}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
export async function getScopedSourceTriples(serviceConfig, config, property, publicationGraph, conceptSchemeUri){

  const { additionalFilter,
          pathToConceptScheme,
          graphsFilter,
          type,
          strictTypeExport,
          healingOptions
        } = config;

  const healingOptionsForProperty = healingOptions && healingOptions[property] ?
      healingOptions[property] : { 'queryChunkSize': 0 };

  let pathToConceptSchemeString = '';

  if(pathToConceptScheme.length){
    const predicatePath = pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');
    pathToConceptSchemeString = `?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.`;
  }

  let strictTypeFilter = '';
  if(property == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type' && strictTypeExport){
    strictTypeFilter = `BIND(${sparqlEscapeUri(type)} as ?object)`;
  }

  // We don't know in what graph the triples are, but we know how they are connected to
  // the concept scheme.
  // What we certainly don't want, are triples only living in the publication-graph
  let bindGraphStatement = ''; // if only one graph needs to be filtered, we bind it for performance
  let graphsFilterStr = `FILTER(?graph NOT IN (${sparqlEscapeUri(publicationGraph)}))`;
  if(graphsFilter.length == 1) {
    bindGraphStatement = `BIND(${sparqlEscapeUri(graphsFilter[0])} as ?graph)`;
  }
  else if(graphsFilter.length > 1){
    //Else use the provided graphs filter
    graphsFilterStr = graphsFilter
      .map(g => `regex(str(?graph), ${sparqlEscapeString(g)})`)
      .join(' || ');
    graphsFilterStr = `FILTER ( ${graphsFilterStr} )`;
  }

  // IMPORTANT NOTE: don't rename "?variables" in this query, as it risks
  // breaking additionalFilter functionality coming from the config file.
  // Yes, this is abstraction leakage. It might be in need in further thinking, but
  // it avoids for now the need for a complicated intermediate abstraction.
  const selectFromDatabase = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)
      ${bindGraphStatement}
      ${strictTypeFilter}
      ?subject a ${sparqlEscapeUri(type)}.
      GRAPH ?graph {
        ?subject ?predicate ?object.
        ${additionalFilter ? additionalFilter : ''}
      }

      ${pathToConceptSchemeString}

      ${bindGraphStatement ? '' : graphsFilterStr}
     }
  `;

  const endpoint = serviceConfig.useVirtuosoForExpensiveSelects ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive queries`);

  const result = await batchedQuery(selectFromDatabase,
                                    healingOptionsForProperty.queryChunkSize,
                                    endpoint
                                   );

  return reformatQueryResult(result, property);
}

export function reformatQueryResult( triples ) {
  return triples.map(t => {
      return {
        nTriple: serializeTriple(t),
        originalFormat: t
      };
    });
}

export async function updateDatabase(serviceConfig, task, operation, updates, extraHeaders, publicationEndpoint, resultFileName, container) {
  console.log(`Starting ${operation.toLowerCase()} batch update`);

  await batchedUpdate(
    updates,
    serviceConfig.publicationGraph,
    operation,
    100,
    serviceConfig.healingInitialBatchSizeInsert,
    extraHeaders,
    publicationEndpoint);

  //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
  await createResultsContainer(serviceConfig, task, updates, container, resultFileName);
}

export async function cleanupHealingJobs(serviceConfig) {
  let healingJobsRetentionCount = serviceConfig.healingJobsRetentionCount;
  if (healingJobsRetentionCount > 0) {
    // cleanup all healing jobs expect the number to keep as defined by 'healingJobsRetentionCount'
    let allJobs = Array.from(await getAllHealingJobs(serviceConfig, serviceConfig));
    let jobsToCleanup = allJobs.slice(healingJobsRetentionCount);
    console.log(`Cleaning up ${jobsToCleanup.length} jobs`)
    for (const job of jobsToCleanup) {
      // for each job, cleanup their tasks
      let tasksToCleanup = await getTasksFromHealingJobs(serviceConfig, job);
      for (const task of tasksToCleanup) {
        // for each task, cleanup their result containers
        let resultContainersToCleanup = await getResultContainersFromTask(serviceConfig, task);
        for (const container of resultContainersToCleanup) {
          // for each container, cleanup their files
          let filesToRemove = await getFilesFromContainer(serviceConfig, container);
          for (const file of filesToRemove) {
            await removeFile(serviceConfig, file);
          }
          await removeDataContainer(serviceConfig, container);
        }
        await removeTask(serviceConfig, task);
      }
      await removeJob(serviceConfig, job);
    }
    // since delta files are not linked to healing jobs, they are removed based on the timestamp of their last update
    // take the timestamp from the oldest retained job to determine the cutoff for the delta files cleanup
    let deltaCleanupTimestamp = await getJobTimestamp(serviceConfig, allJobs[Math.max(healingJobsRetentionCount, allJobs.length)-1])
    let deltaFilesToCleanup = await getDeltaFilesToCleanup(serviceConfig, deltaCleanupTimestamp);
    console.log(`Cleaning up ${deltaFilesToCleanup.length} delta files older than ${deltaCleanupTimestamp}`)
    for (const file of deltaFilesToCleanup) {
      await removeFile(serviceConfig, file);
    }
  }
}

async function getAllHealingJobs(serviceConfig) {
  const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?job WHERE {
      ?job  a ${sparqlEscapeUri(serviceConfig.jobType)};
            task:operation ${sparqlEscapeUri(serviceConfig.healingJobOperation)};
            dct:created ?created.
    } ORDER BY DESC(?created)
  `);
  return result.results.bindings.map(b => b['job'].value);
}

async function getTasksFromHealingJobs(serviceConfig, job) {
  const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?task WHERE {
      ?task dct:isPartOf ${sparqlEscapeUri(job)}.
    }
  `);
  return result.results.bindings.map(b => b['task'].value);
}

async function getResultContainersFromTask(serviceConfig, task) {
  const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?container WHERE {
      ${sparqlEscapeUri(task)} task:resultsContainer ?container.
    }
  `);
  return result.results.bindings.map(b => b['container'].value);
}

async function getFilesFromContainer(serviceConfig, container) {
  const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?file WHERE {
      ${sparqlEscapeUri(container)} task:hasFile ?file.
    }
  `);
  return result.results.bindings.map(b => b['file'].value);
}

async function getJobTimestamp(serviceConfig, job) {
  const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?created WHERE {
        ${sparqlEscapeUri(job)} dct:created ?created.
    }
  `);
  let timestamp = result.results.bindings.map(b => b['created'].value);
  if (timestamp.length === 0) return "0";
  return timestamp[0];
}

async function getDeltaFilesToCleanup(serviceConfig, deltaCleanupTimestamp) {
  const result = await query(`
    ${serviceConfig.prefixes}
    SELECT ?file WHERE {
        ?file a nfo:FileDataObject;
        dct:publisher <${serviceConfig.publisherUri}>;
        dct:modified ?modified.
        FILTER (?modified < "${deltaCleanupTimestamp}"^^xsd:dateTime)
    }
`)
  return result.results.bindings.map(b => b['file'].value);
}

async function removeHelper(serviceConfig, pattern) {
  await update(`
    ${serviceConfig.prefixes}
    DELETE WHERE { GRAPH ?g {
    ${pattern}
    }}`)
}

async function removeFile(serviceConfig, file) {
  const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?source WHERE {
      ?source nie:dataSource ${sparqlEscapeUri(file)}.
    }
  `);
  let fileName = result.results.bindings.map(b => b['source'].value);
  if (fileName.length === 0) return;
  fileName = fileName[0];

  try {
    rmSync(fileName.replace('share://', SHARE_FOLDER))
  } catch (e) {
    console.log(`Removing file ${fileName} failed because of the following error: ${e}, continuing cleanup`)
  }

  await removeHelper(serviceConfig, `
    ${sparqlEscapeUri(fileName)} nie:dataSource ${sparqlEscapeUri(file)}.
  `)
  for (const name of [fileName, file]) {
    // remove the file from the triplestore, since some properties are optional they are split up in different removals
    await removeHelper(serviceConfig, `
    ${sparqlEscapeUri(name)} a nfo:FileDataObject;
      dct:created ?created;
      dct:modified ?modified;
      nfo:fileName ?fileName;
      mu:uuid ?uuid;
      dct:format ?format.
    `)
    // TODO: solve the inconsistent fileExtension property
    await removeHelper(serviceConfig, `
      ${sparqlEscapeUri(name)} dbpedia:fileExtension ?extension.
    `)
    await removeHelper(serviceConfig, `
      ${sparqlEscapeUri(name)} <http://dbpedia.org/ontology/fileExtension> ?extension.
      ${sparqlEscapeUri(name)} dct:creator ?creator.
    `)
    await removeHelper(serviceConfig, `
      ${sparqlEscapeUri(name)} nfo:fileName ?fileName.
    `)
    await removeHelper(serviceConfig, `
      ${sparqlEscapeUri(name)} nfo:fileSize ?size.
    `)
    await removeHelper(serviceConfig, `
      ${sparqlEscapeUri(name)} dct:publisher ?publisher.
    `)
  }
}

async function removeDataContainer(serviceConfig, container) {
  await removeHelper(serviceConfig, `
    ${sparqlEscapeUri(container)} a ?type;
        mu:uuid ?uuid;
        task:hasFile ?file;
        dct:subject ?subject.
  `);
}

async function removeTask(serviceConfig, task) {
  await removeHelper(serviceConfig, `
    ${sparqlEscapeUri(task)} task:resultContainer ?container.
    `)
  await removeHelper(serviceConfig, `
    ${sparqlEscapeUri(task)} a task:Task;
      dct:created ?created;
      dct:modified ?modified;
      mu:uuid ?uuid;
      adms:status ?status;
      task:operation ?operation;
      dct:isPartOf ?job;
      task:index ?index.
    `)
}

async function removeJob(serviceConfig, job) {
  await removeHelper(serviceConfig, `
    ${sparqlEscapeUri(job)} a ${sparqlEscapeUri(serviceConfig.jobType)};
      dct:created ?created;
      dct:modified ?modified;
      mu:uuid ?uuid;
      adms:status ?status;
      task:operation ?operation;
      dct:creator ?creator.
    `)
}
