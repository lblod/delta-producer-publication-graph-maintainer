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
import { uniq } from 'lodash';

const EXPORT_CONFIG = require('/config/export.json');

export async function runHealingTask( task ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(EXPORT_CONFIG.export);

    let accumulatedDiffs = { additions: [], removals: [] };

    //Some explanation:
    // The triples to push to the cache graph should be equal to
    // - all triples whose ?s link to a the conceptscheme (through pathToConceptScheme) and
    // - whose ?p match the properties defined in the EXPORT_CONFIG and
    // - should NOT reside exclusively in the cache graph.
    //
    // In the first step, we build this set (say set A), looking for triples matching the above conditions for a specic ?p.
    // (For performance reasons, we split it up.)
    // In the second step we fetch all triples matching ?p in the cache graph. (set B)
    //
    // With this result, we have a complete picture for a specific ?p to caclulating the difference.
    // The addtions are A\B, and removals are B\A
    for(const property of Object.keys(propertyMap)){

      const sourceTriples = await getSourceTriples(property, propertyMap, conceptSchemeUri);
      const cacheGraphTriples = await getCachedTriples(property, CACHE_GRAPH);
      const diffs = diffNTriples(sourceTriples, cacheGraphTriples);

      accumulatedDiffs.removals = [ ...accumulatedDiffs.removals, ...diffs.removals ];
      accumulatedDiffs.additions = [ ...accumulatedDiffs.additions, ...diffs.additions ];

    }

    if(accumulatedDiffs.removals.length){
      await batchedUpdate(accumulatedDiffs.removals, CACHE_GRAPH, 'DELETE', 500);
      //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
      await createResultsContainer(task, accumulatedDiffs.removals, REMOVAL_CONTAINER, 'removed-triples.ttl');
    }

    if(accumulatedDiffs.additions.length){
      await batchedUpdate(accumulatedDiffs.additions, CACHE_GRAPH, 'INSERT', 500);
      await createResultsContainer(task, accumulatedDiffs.additions, INSERTION_CONTAINER, 'inserted-triples.ttl');
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

function groupPathToConceptSchemePerProperty(config){
  const result = {};
  for( const configEntry of config){
    //TODO: perhaps include this extra predicate in the config file
    const extendedProperties = [...configEntry.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
    for(const property of extendedProperties){
      if(result[property]){
        result[property].push(configEntry.pathToConceptScheme);
      }
      else {
        result[property] = [ configEntry.pathToConceptScheme ];
      }
    }
  }
  return result;
}

async function createResultsContainer( task, nTriples, subject, fileName ){
  const fileContainer = { id: uuid(), subject };
  fileContainer.uri = `http://data.lblod.info/id/dataContainers/${fileContainer.id}`;
  const turtleFile = await writeTtlFile( task.graph , nTriples.join('\n'), fileName);
  await appendTaskResultFile(task, fileContainer, turtleFile);
}

/*
 * Gets the triples for a property, which are considered 'Ground Truth'
 */
async function getSourceTriples( property, propertyMap, conceptSchemeUri ){
  let sourceTriples = [];
  for(const pathToConceptScheme of propertyMap[property]){
    const scopedSourceTriples = await getScopedSourceTriples(pathToConceptScheme,
                                                                   property,
                                                                   conceptSchemeUri,
                                                                   CACHE_GRAPH,
                                                                   EXPORT_CONFIG);

    sourceTriples = [ ...sourceTriples, ...scopedSourceTriples ];
  }
  sourceTriples = uniq(sourceTriples);

  return sourceTriples;
}

/*
 * Gets the triples residing in the cache graph, for a specific property
 */
async function getCachedTriples(property, cacheGraph){
  const selectFromCacheGraph = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      GRAPH ${sparqlEscapeUri(cacheGraph)}{
        ?subject ?predicate ?object.
      }
     }
  `;

  const cacheResult = await batchedQuery(selectFromCacheGraph, 1000);
  const cacheNTriples = cacheResult.map(t => serializeTriple(t));

  return cacheNTriples;

}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the cache graph
 */
async function getScopedSourceTriples( pathToConceptScheme, property, conceptSchemeUri, cacheGraph, exportConfig ){

  const predicatePath = pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');
  //graphsfilter is an optional optimisation step, when it's known WHERE data resides
  const graphsFilter = exportConfig.graphsFilter || [];

  let selectFromDatabase = '';

  if(graphsFilter.length == 1) {

    selectFromDatabase = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)
        GRAPH ${sparqlEscapeUri(graphsFilter[0])}{
          ?subject ?predicate ?object.
        }
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
        ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.
        FILTER(?g NOT IN (${sparqlEscapeUri(cacheGraph)}))
       }
    `;
  }

  const sourceResult = await batchedQuery(selectFromDatabase, 1000);
  const sourceNTriples = sourceResult.map(t => serializeTriple(t));

  return sourceNTriples;
}
