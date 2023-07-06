import { querySudo as query } from '@lblod/mu-auth-sudo';
import * as fs from 'fs';
import * as tmp from 'tmp';
import { execSync } from 'child_process';
import * as Readlines from '@lazy-node/readlines';
import { uniq } from 'lodash';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { writeTtlFile } from '../../lib/file-helpers';
import { appendTaskResultFile } from '../../lib/task';
import { batchedUpdate, batchedQuery, serializeTriple, sparqlEscapePredicate, parseResult } from '../../lib/utils';
import { publishDeltaFiles } from "../../files-publisher/main";
import {
  DELTA_CHUNK_SIZE,
  MU_AUTH_ENDPOINT,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
  VIRTUOSO_ENDPOINT
} from "../../env-config";

const optionsNoOutput = {
  encoding: 'utf-8',
  stdio: ['ignore', 'ignore', 'ignore'],
  shell: '/bin/bash'
};

export async function runHealingTask(serviceConfig, serviceExportConfig, task, isInitialSync, publishDelta ) {

  try {
    const conceptSchemeUri = serviceExportConfig.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(serviceExportConfig.export);

    let accumulatedDiffs;
    if (serviceConfig.useFileDiff) {
      accumulatedDiffs = {inserts: tmp.fileSync(), deletes: tmp.fileSync()};
    } else {
      accumulatedDiffs = {inserts: [], deletes: []};
    }

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

      const sourceTriples = await getTriples(serviceConfig,
                                             serviceExportConfig,
                                             property, propertyMap,
                                             conceptSchemeUri,
                                             getScopedSourceTriples);

      const publicationGraphTriples = await getTriples(serviceConfig,
                                                       serviceExportConfig,
                                                       property, propertyMap,
                                                       conceptSchemeUri,
                                                       getScopedPublicationTriples);

      console.log(`Calculating diffs for property ${property}, this may take a while`);
      if (serviceConfig.useFileDiff) {

        let fileDiff = diffFiles(sourceTriples, publicationGraphTriples);
        let newInserts = tmp.fileSync();

        execSync(`cat ${accumulatedDiffs.inserts.name} ${fileDiff.inserts.name} | tee ${newInserts.name}`, optionsNoOutput);

        fileDiff.inserts.removeCallback();
        accumulatedDiffs.inserts.removeCallback();

        accumulatedDiffs.inserts = newInserts;

        let newDeletes = tmp.fileSync();

        execSync(`cat ${accumulatedDiffs.deletes.name} ${fileDiff.deletes.name} | tee ${newDeletes.name}`, optionsNoOutput);

        fileDiff.deletes.removeCallback();
        accumulatedDiffs.deletes.removeCallback();

        accumulatedDiffs.deletes = newDeletes;

        sourceTriples.removeCallback();
        publicationGraphTriples.removeCallback();
      }
      else {

        let diffs = diffTriplesData(serviceConfig, sourceTriples, publicationGraphTriples);

        accumulatedDiffs.deletes = [ ...accumulatedDiffs.deletes, ...diffs.deletes ];
        accumulatedDiffs.inserts = [ ...accumulatedDiffs.inserts, ...diffs.inserts ];

      }
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

    let fileDiffMaxArraySize = DELTA_CHUNK_SIZE;
    if (serviceConfig.useFileDiff) {
      let deletes = [];
      console.log("Getting data from deletes file");
      let rl = new Readlines(accumulatedDiffs.deletes.name);
      let line, part = 0;
      while ((line = rl.next())) {
        line = line.toString();
        deletes.push(JSON.parse(line).nTriple);
        // to make sure the deletes array does not explode in memory we push the update regularly
        if (deletes.length >= fileDiffMaxArraySize) {
          await updateDatabase(serviceConfig, task, "DELETE", deletes, extraHeaders, publicationEndpoint, `removed-triples-part-${part}.ttl`, serviceConfig.removalContainer);
          deletes = [];
          part++;
        }
      }
      await updateDatabase(serviceConfig, task, "DELETE", deletes, extraHeaders, publicationEndpoint, `removed-triples-part-${part}.ttl`, serviceConfig.removalContainer);
    }
    else {
      let deletes = accumulatedDiffs.deletes.map(t => t.nTriple);
      await updateDatabase(serviceConfig, task, "DELETE", deletes, extraHeaders, publicationEndpoint, 'removed-triples.ttl', serviceConfig.removalContainer);
    }

    if (serviceConfig.useFileDiff) {
      let inserts = [];
      console.log("Getting data from inserts file");
      let rl = new Readlines(accumulatedDiffs.inserts.name);

      let line, part = 0;
      while ((line = rl.next())) {
        line = line.toString();
        inserts.push(JSON.parse(line).nTriple);
        // to make sure the inserts array does not explode in memory we push the update regularly
        if (inserts.length >= fileDiffMaxArraySize) {
          await updateDatabase(serviceConfig, task, "INSERT", inserts, extraHeaders, publicationEndpoint, `inserted-triples-part-${part}.ttl`, serviceConfig.insertionContainer);
          inserts = [];
          part++;
        }
      }
      await updateDatabase(serviceConfig, task, "INSERT", inserts, extraHeaders, publicationEndpoint, `inserted-triples-part-${part}.ttl`, serviceConfig.insertionContainer);
    }
    else {
      let inserts = accumulatedDiffs.inserts.map(t => t.nTriple);
      await updateDatabase(serviceConfig, task, "INSERT", inserts, extraHeaders, publicationEndpoint, 'inserted-triples.ttl', serviceConfig.insertionContainer);
    }

    console.log(`Started at ${started}`);
    console.log(`Ended at ${new Date()}`);
    if (publishDelta) {
      let deletes = [];
      let rl = new Readlines(accumulatedDiffs.deletes.name);
      let line;
      while ((line = rl.next())) {
        line = line.toString();
        deletes.push(JSON.parse(line).nTriple);
        // to make sure the deletes array does not explode in memory we push the update regularly
        if (deletes.length >= fileDiffMaxArraySize) {
          await publishDeltaFiles(serviceConfig, {deletes: deletes, inserts: []});
          deletes = [];
        }
      }
      let inserts = [];
      rl = new Readlines(accumulatedDiffs.inserts.name);
      line = "";
      while ((line = rl.next())) {
        line = line.toString();
        inserts.push(JSON.parse(line).nTriple);
        // to make sure the inserts array does not explode in memory we push the update regularly
        if (inserts.length >= fileDiffMaxArraySize) {
          await publishDeltaFiles(serviceConfig, {inserts: inserts, deletes: []});
          inserts = [];
        }
      }

      // push the remaining inserts and deletes
      await publishDeltaFiles(serviceConfig, {deletes: deletes, inserts: inserts});
    }

    accumulatedDiffs.inserts.removeCallback();
    accumulatedDiffs.deletes.removeCallback();
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
 * Gets the triples for a property
 */
async function getTriples(serviceConfig, serviceExportConfig, property, propertyMap, conceptSchemeUri, getTriplesCall ){
  let sourceTriples;
  if (serviceConfig.useFileDiff) {
    sourceTriples = tmp.fileSync();
  } else {
    sourceTriples = [];
  }
  for(const config of propertyMap[property]){
    let scopedSourceTriples = await getTriplesCall(serviceConfig,
                                                           config,
                                                           property,
                                                           serviceConfig.publicationGraph,
                                                           conceptSchemeUri);
    console.log(`Number of source triples: ${scopedSourceTriples.length}`);

    if (serviceConfig.useFileDiff) {
      let scopedSourceTriplesFile = arrayToFile(scopedSourceTriples, tmp.fileSync());
      const diffs = diffFiles(scopedSourceTriplesFile, sourceTriples);
      let newSourceTriples = tmp.fileSync();
      execSync(`cat ${sourceTriples.name} ${diffs.inserts.name} | tee ${newSourceTriples.name}`, optionsNoOutput);
      diffs.inserts.removeCallback();
      diffs.deletes.removeCallback();
      sourceTriples.removeCallback();
      sourceTriples = newSourceTriples;
      scopedSourceTriplesFile.removeCallback();
    } else {
      const diffs = diffTriplesData(serviceConfig, scopedSourceTriples, sourceTriples);
      sourceTriples = [ ...sourceTriples, ...diffs.inserts ];
    }
  }

  return sourceTriples;
}

/*
 * Gets the triples residing in the publication graph, for a specific property
 */
async function getScopedPublicationTriples(serviceConfig, config, property, publicationGraph){
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
async function getScopedSourceTriples(serviceConfig, config, property, publicationGraph, conceptSchemeUri){

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
  let graphsFilterStr = `FILTER(?graph NOT IN (${sparqlEscapeUri(publicationGraph)}))`;
  if(graphsFilter.length){
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

  const endpoint = serviceConfig.useVirtuosoForExpensiveSelects ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive queries`);

  const result = await batchedQuery(selectFromDatabase,
                                    healingOptionsForProperty.queryChunkSize,
                                    endpoint
                                   );

  return reformatQueryResult(result, property);
}

function arrayToFile(array, file){
  let fd = file.fd;
  for (let i=0; i<array.length; ++i){
    fs.writeSync(fd, JSON.stringify(array[i]) + "\n");
  }
  return file;
}

// read the file and parse each line to an Object, the opposite of the above function
function lines(filename) {
  let retval = [];
  let rl = new Readlines(filename);
  let line;
  while ((line = rl.next())) {
    line = line.toString();
    retval.push(JSON.parse(line));
  }
  return retval;
}

function diffFiles(targetFile, sourceFile, S="50%", T="/tmp"){
  // Note: the S and T parameters can be used to tweak the memory usage of the sort command
  console.log(`Diffing; file based!`);

  let sorted1 = tmp.fileSync();
  let sorted2 = tmp.fileSync();

  execSync(`sort ${targetFile.name} -S ${S} -T ${T} -o ${sorted1.name}`, optionsNoOutput);
  execSync(`sort ${sourceFile.name} -S ${S} -T ${T} -o ${sorted2.name}`, optionsNoOutput);

  let output1 = tmp.fileSync();
  let output2 = tmp.fileSync();

  execSync(`comm -23 ${sorted1.name} ${sorted2.name} | tee ${output1.name}`, optionsNoOutput);
  execSync(`comm -13 ${sorted1.name} ${sorted2.name} | tee ${output2.name}`, optionsNoOutput);

  sorted1.removeCallback();
  sorted2.removeCallback();

  return {
    inserts: output1,
    deletes: output2
  };
}

function diffTriplesData(serviceConfig, target, source) {
  //Note: this only works correctly if triples have same lexical notation.
  //So think about it, when copy pasting :-)

  let diff = { inserts: [], deletes: [] };
  if (target.length === 0) {
    diff.deletes = source;
  } else if (source.length === 0) {
    diff.inserts = target;
  } else if (serviceConfig.useFileDiff) {
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
  } else {
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
  }

  return diff;
}

function reformatQueryResult( triples ) {
  return triples.map(t => {
      return {
        nTriple: serializeTriple(t),
        originalFormat: t
      };
    });
}

async function updateDatabase(serviceConfig, task, operation, updates, extraHeaders, publicationEndpoint, resultFileName, container) {
  console.log(`Starting ${operation.toLowerCase()} batch update`);

  await batchedUpdate(
    updates,
    serviceConfig.publicationGraph,
    operation,
    100,
    serviceConfig.healingPatchGraphBatchSize,
    extraHeaders,
    publicationEndpoint);

  //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
  await createResultsContainer(serviceConfig, task, updates, container, resultFileName);
}
