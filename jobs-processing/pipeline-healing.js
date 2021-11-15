import { uuid, sparqlEscapeString } from 'mu';
import { sparqlEscapeUri } from '../lib/utils';
import { querySudo as query } from '@lblod/mu-auth-sudo';
import { STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         PUBLICATION_GRAPH,
         HEALING_PATCH_GRAPH_BATCH_SIZE,
         INSERTION_CONTAINER,
         REMOVAL_CONTAINER,
         REPORTING_FILES_GRAPH,
         USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS,
         SKIP_MU_AUTH_INITIAL_SYNC,
         VIRTUOSO_ENDPOINT,
         MU_AUTH_ENDPOINT,
         MU_CALL_SCOPE_ID_INITIAL_SYNC,
         MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE
       } from '../env-config';
import {  updateTaskStatus, appendTaskError, appendTaskResultFile } from '../lib/task';
import { sparqlEscapePredicate, batchedUpdate, serializeTriple, loadConfiguration } from '../lib/utils';
import { writeTtlFile } from  '../lib/file-helpers';
import { uniq } from 'lodash';

const EXPORT_CONFIG = loadConfiguration();

export async function runHealingTask( task, isInitialSync ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(EXPORT_CONFIG.export);

    let accumulatedDiffs = { additions: [], removals: [] };

    //Some explanation:
    // The triples to push to the publication graph should be equal to
    // - all triples whose ?s link to a the conceptscheme (through pathToConceptScheme)
    //   (note if no path defined, then this condition returns true) AND
    // - whose ?p match the properties defined in the EXPORT_CONFIG AND
    // - who match any of the configured types AND
    // - (should NOT reside exclusively in the publication graph) XOR (reside in a set of predfined graphs)
    //
    // In the first step, we build this set (say set A), looking for triples matching the above conditions for a specic ?p.
    // (For performance reasons, we split it up.)
    // In the second step we fetch all triples matching ?p in the publication graph. (set B)
    //
    // With this result, we have a complete picture for a specific ?p to caclulating the difference.
    // The addtions are A\B, and removals are B\A
    for(const property of Object.keys(propertyMap)){

      const sourceTriples = await getSourceTriples(property, propertyMap, conceptSchemeUri);
      const publicationGraphTriples = await getPublicationTriples(property, PUBLICATION_GRAPH);

      console.log('Calculating diffs, this may take a while');
      const diffs = diffNTriples(sourceTriples, publicationGraphTriples);

      accumulatedDiffs.removals = [ ...accumulatedDiffs.removals, ...diffs.removals ];
      accumulatedDiffs.additions = [ ...accumulatedDiffs.additions, ...diffs.additions ];

    }

    let extraHeaders = { 'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE };
    if(isInitialSync){
      extraHeaders = { 'mu-call-scope-id': MU_CALL_SCOPE_ID_INITIAL_SYNC };
    }

    let endpoint = MU_AUTH_ENDPOINT;
    if(SKIP_MU_AUTH_INITIAL_SYNC && isInitialSync){
      console.warn(`Skipping mu-auth when injesting data, make sure you know what you're doing.`);
      endpoint = VIRTUOSO_ENDPOINT;
    }

    if(accumulatedDiffs.removals.length){
      await batchedUpdate(accumulatedDiffs.removals,
                          PUBLICATION_GRAPH,
                          'DELETE',
                          500,
                          HEALING_PATCH_GRAPH_BATCH_SIZE,
                          extraHeaders,
                          endpoint
                         );
      //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
      await createResultsContainer(task, accumulatedDiffs.removals, REMOVAL_CONTAINER, 'removed-triples.ttl');
    }

    if(accumulatedDiffs.additions.length){
      await batchedUpdate(accumulatedDiffs.additions,
                          PUBLICATION_GRAPH,
                          'INSERT',
                          500,
                          HEALING_PATCH_GRAPH_BATCH_SIZE,
                          extraHeaders,
                          endpoint
                         );
      await createResultsContainer(task, accumulatedDiffs.additions, INSERTION_CONTAINER, 'inserted-triples.ttl');
    }

    console.log(`started at ${started}`);
    console.log(`ending at ${new Date()}`);
    await updateTaskStatus(task, STATUS_SUCCESS);
  }
  catch(e){
    console.error(e);
    await appendTaskError(task, e.message || e);
    await updateTaskStatus(task, STATUS_FAILED);
  }
}

function groupPathToConceptSchemePerProperty(config){
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

async function createResultsContainer( task, nTriples, subject, fileName ){
  const fileContainer = { id: uuid(), subject };
  fileContainer.uri = `http://data.lblod.info/id/dataContainers/${fileContainer.id}`;
  const turtleFile = await writeTtlFile(REPORTING_FILES_GRAPH || task.graph, nTriples.join('\n'), fileName);
  await appendTaskResultFile(task, fileContainer, turtleFile);
}

/*
 * Gets the triples for a property, which are considered 'Ground Truth'
 */
async function getSourceTriples( property, propertyMap, conceptSchemeUri ){
  let sourceTriples = [];
  for(const config of propertyMap[property]){
    const scopedSourceTriples = await getScopedSourceTriples(config,
                                                             property,
                                                             conceptSchemeUri,
                                                             PUBLICATION_GRAPH,
                                                             EXPORT_CONFIG);

    sourceTriples = [ ...sourceTriples, ...scopedSourceTriples ];
  }
  sourceTriples = uniq(sourceTriples);

  return sourceTriples;
}

/*
 * Gets the triples residing in the publication graph, for a specific property
 */
async function getPublicationTriples(property, publicationGraph){
  const selectFromPublicationGraph = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      GRAPH ${sparqlEscapeUri(publicationGraph)}{
        ?subject ?predicate ?object.
      }
     }
  `;

  //Note: this might explose memory, but now, a paginated fetch is extremely slow. (because sorting)
  const endpoint = USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromPublicationGraph, {}, endpoint);
  let publicationNTriples = [];

  if(result.results && result.results.bindings && result.results.bindings.length){
    const triples = result.results.bindings;
    publicationNTriples = triples.map(t => serializeTriple(t));
  }

  return publicationNTriples;

}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
async function getScopedSourceTriples( config, property, conceptSchemeUri, publicationGraph, exportConfig ){
  const { additionalFilter, pathToConceptScheme, graphsFilter, type } = config;

  let pathToConceptSchemeString = '';

  if(pathToConceptScheme.length){
    const predicatePath = pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');
    pathToConceptSchemeString = `?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.`;
  }

  let selectFromDatabase = '';

  // Important note: renaming variables in the next query, will very likely break
  // additionalFilter functionality. So better leave it as is.
  // This is abstraction leakage, it might be in need in further thinking, but
  // it avoids for now the need for a complicated intermediate abstraction.

  if(graphsFilter.length) {

    const graphsFilterStr = graphsFilter
          .map(g => `regex(str(?graph), ${sparqlEscapeString(g)})`)
          .join(' || ');

    selectFromDatabase = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)
        ?subject a ${sparqlEscapeUri(type)}.
        GRAPH ?graph {
          ?subject ?predicate ?object.
        }

        ${pathToConceptSchemeString}

        ${additionalFilter ? additionalFilter : ''}

        FILTER ( ${graphsFilterStr} )
       }
    `;
  }

  else {
    // We don't know in what graph the triples are, but we know how they are connected to
    // the concept scheme.
    // What we certainly don't want, are triples only living in the publication-graph
    selectFromDatabase = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)
        ?subject a ${sparqlEscapeUri(type)}.
        GRAPH ?graph {
          ?subject ?predicate ?object.
        }
        ${pathToConceptSchemeString}

        ${additionalFilter ? additionalFilter : ''}

        FILTER(?graph NOT IN (${sparqlEscapeUri(publicationGraph)}))
       }
    `;
  }

  //Note: this might explose memory, but now, a paginated fetch is extremely slow. (because sorting)
  const endpoint = USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromDatabase, {}, endpoint);
  let sourceNTriples = [];

  if(result.results && result.results.bindings && result.results.bindings.length){
    const triples = result.results.bindings;
    sourceNTriples = triples.map(t => serializeTriple(t));
  }

  return sourceNTriples;
}

function diffNTriples(target, source) {
  //Note: this only works correctly if triples have same lexical notation.
  //So think about it, when copy pasting :-)
  const diff = { additions: [], removals: [] };

  const targetHash = target.reduce((acc, curr) => {
    acc[curr] = curr;
    return acc;
  }, {});

  const sourceHash = source.reduce((acc, curr) => {
    acc[curr] = curr;
    return acc;
  }, {});

  diff.additions = target.filter(nt => !sourceHash[nt]);
  diff.removals = source.filter(nt => !targetHash[nt]);

  return diff;
}
