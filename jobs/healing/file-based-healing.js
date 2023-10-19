import * as tmp from 'tmp';
import * as Readlines from '@lazy-node/readlines';
import { publishDeltaFiles } from "../../files-publisher/main";
import { appendPublicationGraph } from '../utils';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';

import {
  groupPathToConceptSchemePerProperty,
  getScopedSourceTriples,
  getScopedPublicationTriples,
  updateDatabase,
  generateGetPublicationTriplesQuery,
  generateGetSourceTriplesQuery,
  reformatQueryResult
}  from './utils';

import {
  diffFiles,
  mergeFiles,
  arrayToFile,
  lines
} from './file-utils';

import {
  MU_AUTH_ENDPOINT,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
  VIRTUOSO_ENDPOINT
} from "../../env-config";

/*
 * This function runs the healing task by using file based diffing.
 * Although very similar to standard-healing.js, the boilerplate is different
 */
export async function runHealingTask(serviceConfig, serviceExportConfig, task, isInitialSync, publishDelta) {

  try {
    const conceptSchemeUri = serviceExportConfig.conceptScheme;
    const started = new Date();
    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(serviceExportConfig.export);

    let accumulatedDiffs = { inserts: tmp.fileSync(), deletes: tmp.fileSync() };

    // See standard-healing.js for more explanation
    for(const property of Object.keys(propertyMap)){

      let endpoint = serviceConfig.useVirtuosoForExpensiveSelects ?
          VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
      const sourceTriples = await getTriples(serviceConfig,
                                             property, propertyMap,
                                             conceptSchemeUri,
                                             endpoint,
                                             generateGetSourceTriplesQuery);

      endpoint = serviceConfig.useVirtuosoForExpensiveSelects ?
        PUBLICATION_VIRTUOSO_ENDPOINT : PUBLICATION_MU_AUTH_ENDPOINT;
      const publicationGraphTriples = await getTriples(serviceConfig,
                                                       property, propertyMap,
                                                       conceptSchemeUri,
                                                       endpoint,
                                                       generateGetPublicationTriplesQuery);

      console.log(`Calculating diffs for property ${property}, this may take a while`);
      let fileDiff = diffFiles(sourceTriples, publicationGraphTriples);

      accumulatedDiffs.inserts = mergeFiles(accumulatedDiffs.inserts, fileDiff.inserts, true);
      accumulatedDiffs.deletes = mergeFiles(accumulatedDiffs.deletes, fileDiff.deletes, true);

      sourceTriples.removeCallback();
      publicationGraphTriples.removeCallback();
    }

    let extraHeaders = { 'mu-call-scope-id': serviceConfig.muCallScopeIdPublicationGraphMaintenance };

    if(isInitialSync){
      extraHeaders = { 'mu-call-scope-id': serviceConfig.muCallScopeIdInitialSync };
    }

    let publicationEndpoint = PUBLICATION_MU_AUTH_ENDPOINT;

    if(serviceConfig.skipMuAuthInitialSync && isInitialSync){
      console.warn(`Skipping mu-auth when ingesting data, make sure you know what you're doing.`);
      publicationEndpoint = PUBLICATION_VIRTUOSO_ENDPOINT;
    }
    else if(serviceConfig.skipMuAuthHealing && !isInitialSync) {
      console.warn(`Skipping mu-auth when healing data, make sure you know what you're doing.`);
      publicationEndpoint = PUBLICATION_VIRTUOSO_ENDPOINT;
    }

    let fileDiffMaxArraySize = serviceConfig.healingMaxTriplesInMemory;

    await updatePublicationGraph(
      "DELETE",
      serviceConfig,
      extraHeaders,
      accumulatedDiffs.deletes,
      task,
      publicationEndpoint,
      fileDiffMaxArraySize
    );

    await updatePublicationGraph(
      "INSERT",
      serviceConfig,
      extraHeaders,
      accumulatedDiffs.inserts,
      task,
      publicationEndpoint,
      fileDiffMaxArraySize
    );

    console.log(`Started at ${started}`);
    console.log(`Ended at ${new Date()}`);

    if (publishDelta && !isInitialSync) {
      await pushToDeltaFiles(serviceConfig, "DELETE", accumulatedDiffs.deletes, fileDiffMaxArraySize);
      await pushToDeltaFiles(serviceConfig, "INSERT", accumulatedDiffs.inserts, fileDiffMaxArraySize);
    }

    accumulatedDiffs.inserts.removeCallback();
    accumulatedDiffs.deletes.removeCallback();
  }
  catch(e){
    console.error(e);
    throw e;
  }
}

/*
 * Gets the triples for a property
 */
async function getTriples(serviceConfig, property, propertyMap, conceptSchemeUri, endpoint, generateQuery ){
  let sourceTriples = tmp.fileSync();
  console.log(`Hitting database ${endpoint} with expensive queries`);

  for(const config of propertyMap[property]){
    let queryStr = generateQuery(
      { config,
        property,
        publicationGraph: serviceConfig.publicationGraph,
        conceptSchemeUri,
        asConstructQuery: true
      });

    const results = await query(queryStr, {}, { sparqlEndpoint: endpoint, mayRetry: true });

    let triples = results?.results?.bindings.map(b => {
      return  {
        graph: { type: 'uri', value: 'http://mu.semte.ch/graphs/stub' }, // Since construct query, we don't have this info
        subject: b.s,
        predicate: b.p,
        object: b.o
      };
    }) || [];
    triples = reformatQueryResult(triples);

    let scopedSourceTriplesFile = arrayToFile(triples, tmp.fileSync());
    const diffs = diffFiles(scopedSourceTriplesFile, sourceTriples);

    sourceTriples = mergeFiles(sourceTriples, diffs.inserts, true);

    diffs.inserts.removeCallback();
    diffs.deletes.removeCallback();
    scopedSourceTriplesFile.removeCallback();

  }

  return sourceTriples;
}

function diffTriplesData(serviceConfig, target, source) {
  //Note: this only works correctly if triples have same lexical notation.
  //So think about it, when copy pasting :-)
  let diff = { inserts: [], deletes: [] };
  if (target.length === 0) {
    diff.deletes = source;
  }
  else if (source.length === 0) {
    diff.inserts = target;
  }
  else {
    console.log(`File based diff: target size is ${target.length}, source size is ${source.length}`);
    // only do the file-based diff when the dataset is large, since otherwise the overhead is too much
    let targetFile = arrayToFile(target, tmp.fileSync());
    let sourceFile = arrayToFile(source, tmp.fileSync());
    let fileDiff = diffFiles(targetFile, sourceFile);
    console.log(`File based diff: calculating inserts and deletes from files`);

    diff = {
      inserts: lines(fileDiff.inserts.name),
      deletes: lines(fileDiff.deletes.name)
    };

    fileDiff.inserts.removeCallback();
    fileDiff.deletes.removeCallback();
    targetFile.removeCallback();
    sourceFile.removeCallback();
  }
  return diff;
}

async function updatePublicationGraph(
  operation,
  serviceConfig,
  extraHeaders,
  filePointer,
  task,
  publicationEndpoint,
  fileDiffMaxArraySize
) {

  let triples = [];
  console.log(`Getting data from ${filePointer.name} for operation ${operation}`);
  const container = operation == "DELETE" ? serviceConfig.removalContainer : serviceConfig.insertionContainer;
  let rl = new Readlines(filePointer.name);
  let line, part = 0;
  let linesCounter = 0;

  while ((line = rl.next())) {
    line = line.toString();
    triples.push(JSON.parse(line).nTriple);
    linesCounter++;
    // to make sure the deletes array does not explode in memory we push the update regularly
    if (linesCounter >= fileDiffMaxArraySize) {
      await updateDatabase(
        serviceConfig,
        task,
        operation,
        triples,
        extraHeaders,
        publicationEndpoint,
        `${operation}-triples-part-${part}.ttl`,
        container
     );
      triples = [];
      part++;
      linesCounter = 0;
    }
  }

  await updateDatabase(
    serviceConfig,
    task,
    operation,
    triples,
    extraHeaders,
    publicationEndpoint,
    `${operation}-triples-part-${part}.ttl`,
    container
  );
}

async function pushToDeltaFiles(serviceConfig, operation, filePointer, fileDiffMaxArraySize) {
  let triples = [];
  let rl = new Readlines(filePointer.name);
  let line, linesCounter = 0; //keep track of the amount of lines read, since performance
  while ((line = rl.next())) {
    line = line.toString();
    triples.push(JSON.parse(line).originalFormat);
    linesCounter++;
    if (linesCounter >= fileDiffMaxArraySize) {
      // to make sure the deletes does not explode in memory we push the update regularly
      triples = triples.map(t => appendPublicationGraph(serviceConfig, t));
      const data = operation == "DELETE" ?
            { deletes: triples, inserts: []} : {deletes: [], inserts: triples};
      await publishDeltaFiles(serviceConfig, data, true);
      triples = [];
      linesCounter = 0;
    }
  }

  // push the remaining inserts and deletes
  triples = triples.map(t => appendPublicationGraph(serviceConfig, t));
  const data = operation == "DELETE" ?
        { deletes: triples, inserts: []} : {deletes: [], inserts: triples};
  await publishDeltaFiles(serviceConfig, data, true);
}
