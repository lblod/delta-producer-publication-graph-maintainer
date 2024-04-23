import { uniq } from 'lodash';
import { appendTaskResultFile } from '../../lib/task';
import { writeTtlFile } from '../../lib/file-helpers';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { batchedUpdate, batchedQuery, sparqlEscapePredicate, serializeTriple } from '../../lib/utils';

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

export function generateGetPublicationTriplesQuery({ config, property, publicationGraph, asConstructQuery = false }) {

  const resultsExpression = asConstructQuery ?
        `CONSTRUCT { ?subject ?predicate ?object }` :
        'SELECT DISTINCT ?subject ?predicate ?object';

  const { type } = config;
  return `
   ${resultsExpression} WHERE {
    GRAPH ${sparqlEscapeUri(publicationGraph)}{
      BIND(${sparqlEscapeUri(property)} as ?predicate)
      ?subject a ${sparqlEscapeUri(type)}.
      ?subject ?predicate ?object.
    }
   }`;
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

  const selectFromPublicationGraph = generateGetPublicationTriplesQuery({ config, property, publicationGraph });

  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await batchedQuery(selectFromPublicationGraph,
                                    healingOptionsForProperty.queryChunkSize,
                                    endpoint
                                   );

  return reformatQueryResult(result, property);
}

export function generateGetSourceTriplesQuery({
  config,
  property,
  publicationGraph,
  conceptSchemeUri,
  asConstructQuery = false
}) {
  const { additionalFilter,
          pathToConceptScheme,
          graphsFilter,
          hasRegexGraphsFilter,
          type,
          strictTypeExport,
          healingOptions
        } = config;

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
  if(!hasRegexGraphsFilter) {
    if(graphsFilter.length == 1) {
      bindGraphStatement = `BIND(${sparqlEscapeUri(graphsFilter[0])} as ?graph)`;
    }
    else if(graphsFilter.length > 1) {
      const graphsSetString = graphsFilter.map(g => sparqlEscapeUri(g)).join(',\n');
      graphsFilterStr = `FILTER(?graph IN (${graphsSetString}))`;
    }
  }
  else if(hasRegexGraphsFilter && graphsFilter.length > 0) {
    graphsFilterStr = graphsFilter
      .map(g => `regex(str(?graph), ${sparqlEscapeString(g)})`)
      .join(' || ');
    graphsFilterStr = `FILTER ( ${graphsFilterStr} )`;
  }

  const resultsExpression = asConstructQuery ?
        `CONSTRUCT { ?subject ?predicate ?object }` :
        'SELECT DISTINCT ?subject ?predicate ?object';

  // IMPORTANT NOTE: don't rename "?variables" in this query, as it risks
  // breaking additionalFilter functionality coming from the config file.
  // Yes, this is abstraction leakage. It might be in need in further thinking, but
  // it avoids for now the need for a complicated intermediate abstraction.
  const queryForSourceData = `
    ${resultsExpression} WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)
      ${bindGraphStatement}
      ${strictTypeFilter}
      ?subject a ${sparqlEscapeUri(type)}.
      GRAPH ?graph {
        ?subject ?predicate ?object.
      }

      ${additionalFilter ? additionalFilter : ''}

      ${pathToConceptSchemeString}

      ${bindGraphStatement ? '' : graphsFilterStr}
     }
  `;

  return queryForSourceData;
}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
export async function getScopedSourceTriples(serviceConfig, config, property, publicationGraph, conceptSchemeUri){
  const {  healingOptions } = config;

  const healingOptionsForProperty = healingOptions && healingOptions[property] ?
        healingOptions[property] : { 'queryChunkSize': 0 };

  const endpoint = serviceConfig.useVirtuosoForExpensiveSelects ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive queries`);

  const selectFromDatabase = generateGetSourceTriplesQuery({config, property, publicationGraph, conceptSchemeUri});

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
