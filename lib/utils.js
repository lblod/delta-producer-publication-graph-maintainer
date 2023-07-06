import { uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { chunk } from 'lodash';
import {MU_AUTH_ENDPOINT} from "../env-config";

export function isInverse(predicate) {
  return predicate && predicate.startsWith('^');
}

export function sparqlEscapePredicate(predicate) {
  return isInverse(predicate) ? `^<${predicate.slice(1)}>` : `<${predicate}>`;
}

export function normalizePredicate(predicate) {
  return isInverse(predicate) ? predicate.slice(1) : predicate;
}

export function serializeTriple(triple) {
  const predicate = sparqlEscapePredicate(triple.predicate.value);
  return `${serializeTriplePart(triple.subject)} ${predicate} ${serializeTriplePart(triple.object)}.`;
}

export function serializeTriplePart(triplePart){
  if(triplePart.type == 'uri' || triplePart.termType == "NamedNode"){
    return sparqlEscapeUri(triplePart.value);
  }
  else if (triplePart.type === 'literal' || triplePart.type === 'typed-literal') {
    if(triplePart.datatype) {
        return `${sparqlEscapeString(triplePart.value)}^^${sparqlEscapeUri(triplePart.datatype)}`;
    }
    else if(triplePart.lang) {
      return `${sparqlEscapeString(triplePart.value)}@${triplePart.lang}`;
    }
    else {
      return sparqlEscapeString(triplePart.value);
    }
  }
  else {
    console.log(`Don't know how to escape type ${triplePart.type}. Will escape as a string.`);
    return sparqlEscapeString(triplePart.value);
  }
}

/**
 * Returns whether 2 arrays of path segments (as string) are equal
*/
export function isSamePath(a, b) {
  return a.join('/') == b.join('/');
}

/**
 * convert results of select query to an array of objects.
 * courtesy: Niels Vandekeybus & Felix
 * @method parseResult
 * @return {Array}
 */
export function parseResult( result ) {
  if(!(result.results && result.results.bindings.length)) return [];

  const bindingKeys = result.head.vars;
  return result.results.bindings.map((row) => {
    const obj = {};
    bindingKeys.forEach((key) => {
      if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#integer' && row[key].value){
        obj[key] = parseInt(row[key].value);
      }
      else if(row[key] && row[key].datatype == 'http://www.w3.org/2001/XMLSchema#dateTime' && row[key].value){
        obj[key] = new Date(row[key].value);
      }
      else obj[key] = row[key] ? row[key].value:undefined;
    });
    return obj;
  });
};


export async function batchedUpdate(nTriples, targetGraph,
                                     operation = 'INSERT', sleep = 1000,
                                     batch = 100, extraHeaders = {},
                                     endpoint = MU_AUTH_ENDPOINT) {
  const chunkedArray = chunk(nTriples, batch);
  for(const chunks of chunkedArray){
    const updateQuery = `
      ${operation} DATA {
         GRAPH ${sparqlEscapeUri(targetGraph)} {
           ${chunks.join('\n')}
         }
      }
    `;
    console.log(`Hitting database ${endpoint} with batched query`);
    await update(updateQuery, extraHeaders, { sparqlEndpoint: endpoint, mayRetry: true });
    console.log(`Sleeping before next query execution: ${sleep}`);
    await new Promise(r => setTimeout(r, sleep));
  }
}

async function countResultSet(targetQuery){
  const countQuery = `
     SELECT (COUNT(*) as ?total) {
        {
          ${targetQuery}
        }
     }
   `;

  const result = await query(countQuery);

  if(!(result.results || result.results.bindings.length)){
    return 0;
  }
  else {
    return parseResult(result)[0].total;
  }
}

export async function storeError(service_config, errorMsg) {
  const id = uuid();
  const uri = service_config.errorUriPrefix + id;

  const queryError = `
    ${service_config.prefixes}

    INSERT DATA {
      GRAPH ${sparqlEscapeUri(service_config.jobsGraph)}{
        ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(service_config.errorType)}, ${sparqlEscapeUri(service_config.deltaErrorType)};
          mu:uuid ${sparqlEscapeString(id)};
          dct:subject "Delta Producer Publication Graph Maintainer" ;
          oslc:message ${sparqlEscapeString(errorMsg)};
          dct:created ${sparqlEscapeDateTime(new Date().toISOString())} ;
          dct:creator ${sparqlEscapeUri(service_config.errorCreatorUri)} .
      }
    }
  `;

  await update(queryError);
}

export async function batchedQuery(subjectPredicateObjectQuery,
                                    batch = 100,
                                    sparqlEndpoint = MU_AUTH_ENDPOINT,
                                    orderByStr = 'ORDER BY ?subject ?predicate ?object',
                                  ){
  if(!batch || batch == 0 || batch < 0) {
    const result = await query(subjectPredicateObjectQuery, {}, { sparqlEndpoint: sparqlEndpoint, mayRetry: true });
    const triples = result.results.bindings;
    return triples;
  }
  else {
    const numberOfResults = await countResultSet(subjectPredicateObjectQuery);
    console.log(`Taking about ${numberOfResults} triples here`);

    let offset = 0;
    let allTriples = [];
    while(offset <= numberOfResults){
      const paginatedQuery = `
        ${subjectPredicateObjectQuery}
        ${orderByStr}
        OFFSET ${offset}
        LIMIT ${batch}
      `;

      const result = await query(paginatedQuery, {}, { sparqlEndpoint: sparqlEndpoint, mayRetry: true });
      const triples = result.results.bindings;
      if(triples.length){
        allTriples = [...allTriples, ...triples ];
      }
      offset += batch;
    }
    return allTriples;
  }
}

export function loadConfiguration(path){
  const config = require(path);

  //Make sure it is 'syntactically' correct
  if(!config.conceptScheme) {
    config['conceptScheme'] = undefined;
  }
  if(!config.export || !config.export.length) {
    throw 'No correct export, or resources to export found!';
  }
  else {
    for(const exportConfig of config.export){
      if(!exportConfig.type){
        throw `No type found for ${JSON.stringify(exportConfig)}`;
      }
      else if(exportConfig.pathToConceptScheme && exportConfig.pathToConceptScheme.length && !config.conceptScheme){
        throw `pathToConceptScheme has been found for ${JSON.stringify(exportConfig)}, but no target conceptScheme`;
      }
      else if(!exportConfig.properties || !exportConfig.properties.length){
        throw `No properties found to export for ${JSON.stringify(exportConfig)}`;
      }
      else {
        if(!exportConfig.pathToConceptScheme){
          exportConfig.pathToConceptScheme = [];
        }
        if(!exportConfig.graphsFilter){
          exportConfig.graphsFilter = [];
        }
      }
    }
  }
  return config;
}
