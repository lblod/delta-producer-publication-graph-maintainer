import { querySudo as query } from '@lblod/mu-auth-sudo';
import { uniq } from 'lodash';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { writeTtlFile } from '../../lib/file-helpers';
import { appendTaskResultFile } from '../../lib/task';
import { batchedUpdate, serializeTriple, sparqlEscapePredicate } from '../../lib/utils';
import { appendPublicationGraph } from '../utils';
import {
  MU_AUTH_ENDPOINT,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
  VIRTUOSO_ENDPOINT
} from "../../env-config";


export async function runHealingTask(serviceConfig, serviceExportConfig, task, isInitialSync) {
  try {
    const conceptSchemeUri = serviceExportConfig.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(serviceExportConfig.export);

    let accumulatedDiffs = { inserts: [], deletes: [] };

    //Some explanation:
    // The triples to push to the publication graph should be equal to
    // - all triples whose ?s link to the concept scheme (through pathToConceptScheme)
    //   (note if no path defined, then this condition returns true) AND
    // - whose ?p match the properties defined in the serviceExportConfig AND
    // - who match any of the configured types AND
    // - (should NOT reside exclusively in the publication graph) XOR (reside in a set of predefined graphs)
    //
    // In the first step, we build this set (say set A), looking for triples matching the above conditions for a specific ?p.
    // (For performance reasons, we split it up.)
    // In the second step we fetch all triples matching ?p in the publication graph. (set B)
    //
    // With this result, we have a complete picture for a specific ?p to calculating the difference.
    // The additions are A\B, and removals are B\A
    for(const property of Object.keys(propertyMap)){

      const sourceTriples = await getSourceTriples(serviceConfig, serviceExportConfig, property, propertyMap, conceptSchemeUri);
      const publicationGraphTriples = await getPublicationTriples(serviceConfig, property, serviceConfig.publicationGraph);

      console.log('Calculating diffs, this may take a while');
      const diffs = diffTriplesData(sourceTriples, publicationGraphTriples);

      accumulatedDiffs.deletes = [ ...accumulatedDiffs.deletes, ...diffs.deletes ];
      accumulatedDiffs.inserts = [ ...accumulatedDiffs.inserts, ...diffs.inserts ];

    }

    let extraHeaders = { 'mu-call-scope-id': serviceConfig.muCallScopeIdPublicationGraphMaintenance };
    if(isInitialSync){
      extraHeaders = { 'mu-call-scope-id': serviceConfig.muCallScopeIdInitialSync };
    }

    let publicationEndpoint = PUBLICATION_MU_AUTH_ENDPOINT;
    if(serviceConfig.skipMuAuthInitialSync && isInitialSync){
      console.warn(`Skipping mu-auth when injesting data, make sure you know what you're doing.`);
      publicationEndpoint = PUBLICATION_VIRTUOSO_ENDPOINT;
    }

    if(accumulatedDiffs.deletes.length){
      const deletes = accumulatedDiffs.deletes.map(t => t.nTriple);
      await batchedUpdate(serviceConfig, deletes,
                          serviceConfig.publicationGraph,
                          'DELETE',
                          500,
                          serviceConfig.healingPatchGraphBatchSize,
                          extraHeaders,
                          publicationEndpoint
                         );
      //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
      await createResultsContainer(serviceConfig, task, deletes, serviceConfig.removalContainer, 'removed-triples.ttl');
    }

    if(accumulatedDiffs.inserts.length){
      const inserts = accumulatedDiffs.inserts.map(t => t.nTriple);
      await batchedUpdate(serviceConfig, inserts,
                          serviceConfig.publicationGraph,
                          'INSERT',
                          500,
                          serviceConfig.healingPatchGraphBatchSize,
                          extraHeaders,
                          publicationEndpoint
                         );
      await createResultsContainer(serviceConfig, task, inserts, serviceConfig.insertionContainer, 'inserted-triples.ttl');
    }

    console.log(`started at ${started}`);
    console.log(`ending at ${new Date()}`);
    return {
             inserts: accumulatedDiffs.inserts.map(t => appendPublicationGraph(serviceConfig, t.originalFormat)),
             deletes: accumulatedDiffs.deletes.map(t => appendPublicationGraph(serviceConfig, t.originalFormat))
           };
  }
  catch(e){
    console.error(e);
    throw e;
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

async function createResultsContainer(serviceConfig, task, nTriples, subject, fileName ){
  const fileContainer = { id: uuid(), subject };
  fileContainer.uri = `http://data.lblod.info/id/dataContainers/${fileContainer.id}`;
  const turtleFile = await writeTtlFile(serviceConfig.reportingFilesGraph || task.graph, nTriples.join('\n'), fileName);
  await appendTaskResultFile(task, fileContainer, turtleFile);
}

/*
 * Gets the triples for a property, which are considered 'Ground Truth'
 */
async function getSourceTriples(serviceConfig, serviceExportConfig, property, propertyMap, conceptSchemeUri ){
  let sourceTriples = [];
  for(const config of propertyMap[property]){
    const scopedSourceTriples = await getScopedSourceTriples(serviceConfig,
                                                             config,
                                                             property,
                                                             conceptSchemeUri,
                                                             serviceConfig.publicationGraph,
                                                             serviceExportConfig);

    const diffs = diffTriplesData(scopedSourceTriples, sourceTriples);
    sourceTriples = [ ...sourceTriples, ...diffs.inserts ];
  }

  return sourceTriples;
}

/*
 * Gets the triples residing in the publication graph, for a specific property
 */
async function getPublicationTriples(serviceConfig, property, publicationGraph){
  const selectFromPublicationGraph = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      GRAPH ${sparqlEscapeUri(publicationGraph)}{
        ?subject ?predicate ?object.
      }
     }
  `;

  //Note: this might explode memory, but now, a paginated fetch is extremely slow. (because sorting)
  const endpoint = serviceConfig.useVirtuosoForExpensiveSelects ? PUBLICATION_VIRTUOSO_ENDPOINT : PUBLICATION_MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromPublicationGraph, {}, { sparqlEndpoint: endpoint, mayRetry: true });
  return reformatQueryResult(result);
}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
async function getScopedSourceTriples(serviceConfig, config, property, conceptSchemeUri, publicationGraph, exportConfig ){
  const { additionalFilter, pathToConceptScheme, graphsFilter, type, strictTypeExport } = config;

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
  let graphsFilterStr = `FILTER(?graph NOT IN (${sparqlEscapeUri(publicationGraph)}))`;
  if(graphsFilter.length){
    //Else use the provided graphs filter
    graphsFilterStr = graphsFilter
      .map(g => `regex(str(?graph), ${sparqlEscapeString(g)})`)
      .join(' || ');
    graphsFilterStr = `FILTER ( ${graphsFilterStr} )`;
  }

  // Important note: renaming variables in the next query, will very likely break
  // additionalFilter functionality. So better leave it as is.
  // This is abstraction leakage, it might be in need in further thinking, but
  // it avoids for now the need for a complicated intermediate abstraction.

  const selectFromDatabase = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)
      ${strictTypeFilter}
      ?subject a ${sparqlEscapeUri(type)}.
      GRAPH ?graph {
        ?subject ?predicate ?object.
        ${additionalFilter ? additionalFilter : ''}
      }

      ${pathToConceptSchemeString}

      ${graphsFilterStr}
     }
  `;

  //Note: this might explode memory, but now, a paginated fetch is extremely slow. (because sorting)
  const endpoint = serviceConfig.useVirtuosoForExpensiveSelects ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromDatabase, {}, { sparqlEndpoint: endpoint, mayRetry: true });
  return reformatQueryResult(result);
}

function diffTriplesData(target, source) {
  //Note: this only works correctly if triples have same lexical notation.
  //So think about it, when copy pasting :-)
  const diff = { inserts: [], deletes: [] };

  const targetHash = target.reduce((acc, curr) => {
    acc[curr.nTriple] = curr;
    return acc;
  }, {});

  const sourceHash = source.reduce((acc, curr) => {
    acc[curr.nTriple] = curr;
    return acc;
  }, {});

  diff.inserts = target.filter(nt => !sourceHash[nt.nTriple]);
  diff.deletes = source.filter(nt => !targetHash[nt.nTriple]);

  return diff;
}

function reformatQueryResult( result ){
  let triplesData = [];

  if(result.results && result.results.bindings && result.results.bindings.length){
    const triples = result.results.bindings;
    triplesData = triples.map(t => {
      return {
        nTriple: serializeTriple(t),
        originalFormat: t
      };
    });
  }

  return triplesData;
}
