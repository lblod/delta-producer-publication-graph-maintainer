import { uuid, sparqlEscapeUri } from 'mu';
import { STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         CACHE_GRAPH,
         INSERTION_CONTAINER,
         REMOVAL_CONTAINER
       } from '../env-config';
import {  updateTaskStatus, appendTaskError, appendTaskResultFile } from '../lib/task';
import { sparqlEscapePredicate, batchedQuery, batchedUpdate, diffNTriples, serializeTriple } from '../lib/utils';
import { writeTtlFile } from  '../lib/file-helpers';

const EXPORT_CONFIG = require('/config/export.json');

export async function runHealingTask( task ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;
    const started = new Date();
    console.log(`starting at ${started}`);
    for( const config of EXPORT_CONFIG.export){
      //TODO: perhaps include this extra predicate in the config file
      const extendedProperties = [...config.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
      for(const property of extendedProperties){
        //Prefer slow over everything at once
        const diffs = await calculateDiffs(conceptSchemeUri, config, CACHE_GRAPH, config.type, property);
        if(diffs.onlyInCacheGraph.length){
          await batchedUpdate(diffs.onlyInCacheGraph, CACHE_GRAPH, 'DELETE', 500);
          //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
          await createResultsContainer(task, diffs.onlyInCacheGraph, REMOVAL_CONTAINER, 'removed-triples.ttl');
        }
        if(diffs.onlyInSource.length){
          await batchedUpdate(diffs.onlyInSource, CACHE_GRAPH, 'INSERT', 500);
          await createResultsContainer(task, diffs.onlyInSource, INSERTION_CONTAINER, 'inserted-triples.ttl');
        }
      }
    }

    console.log(`started at ${started}`);
    console.log(`ending at ${new Date()}`);
    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

async function createResultsContainer( task, nTriples, subject, fileName ){
  const fileContainer = { id: uuid(), subject };
  fileContainer.uri = `http://data.lblod.info/id/dataContainers/${fileContainer.id}`;
  const turtleFile = await writeTtlFile( task.graph , nTriples.join('\n'), fileName);
  await appendTaskResultFile(task, fileContainer, turtleFile);
}

async function calculateDiffs( conceptSchemeUri, config, cacheGraph, type, property ){
  //Some optimisations were needed because diff graphs is heavy on the database.
  const predicatePath = config.pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');
  const graphsFilter = config.graphsFilter || [];

  if(graphsFilter.length > 1){
    throw `Currently the max supported length of graphsFilter is <= 1, see ${JSON.stringify(config)}`;
  }
  else {
    let selectFromDatabase = '';

    //Performance optimisation path: we know what graph we want. This graph + predicatePath will be ground truth
    if(graphsFilter.length == 1) {

      selectFromDatabase = `
        SELECT DISTINCT ?subject ?predicate ?object WHERE {
          BIND(${sparqlEscapeUri(property)} as ?predicate)
          GRAPH ${sparqlEscapeUri(graphsFilter[0])}{
            ?subject ?predicate ?object.
          }
          ?subject a ${sparqlEscapeUri(type)}.
          ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.
         }
      `;
    }

    else {
      // We don't know in what graph the triples are, but we know how they are connected to
      // the concept scheme.
      // What we certainly don't want, are triples only living in the cache-graph
      selectFromDatabase = `
        SELECT DISTINCT ?subject ?predicate ?object WHERE {
          BIND(${sparqlEscapeUri(property)} as ?predicate)
          GRAPH ?g {
            ?subject ?predicate ?object.
          }
          ?subject a ${sparqlEscapeUri(type)}.
          ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.
          FILTER(?g NOT IN (${sparqlEscapeUri(cacheGraph)}))
         }
      `;
     }

    const sourceResult = await batchedQuery(selectFromDatabase, 1000);
    const sourceNTriples = sourceResult.map(t => serializeTriple(t));

    const selectFromCacheGraph = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)

        GRAPH ${sparqlEscapeUri(cacheGraph)}{
          ?subject a ${sparqlEscapeUri(type)}.
          ?subject ?predicate ?object.
        }
       }
    `;

    const cacheResult = await batchedQuery(selectFromCacheGraph, 1000);
    const cacheNTriples = cacheResult.map(t => serializeTriple(t));

    const tripleDiffs = diffNTriples(cacheNTriples, sourceNTriples);

    return { onlyInCacheGraph: tripleDiffs.additions, onlyInSource: tripleDiffs.removals };
  }
}
