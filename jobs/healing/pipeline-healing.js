import {querySudo as query} from '@lblod/mu-auth-sudo';
import * as fs from 'fs';
import * as tmp from 'tmp';
import {execSync} from 'child_process';
import * as Readlines from '@lazy-node/readlines';
import {uniq} from 'lodash';
import {sparqlEscapeString, sparqlEscapeUri, uuid} from 'mu';
import {
  HEALING_PATCH_GRAPH_BATCH_SIZE,
  INSERTION_CONTAINER,
  MU_AUTH_ENDPOINT,
  MU_CALL_SCOPE_ID_INITIAL_SYNC,
  MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE,
  PUBLICATION_GRAPH,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
  REMOVAL_CONTAINER,
  REPORTING_FILES_GRAPH,
  SKIP_MU_AUTH_INITIAL_SYNC,
  USE_FILE_DIFF,
  USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS,
  VIRTUOSO_ENDPOINT
} from '../../env-config';
import {writeTtlFile} from '../../lib/file-helpers';
import {appendTaskResultFile} from '../../lib/task';
import {batchedUpdate, loadConfiguration, serializeTriple, sparqlEscapePredicate} from '../../lib/utils';
import {publishDeltaFiles} from "../../files-publisher/main";


const EXPORT_CONFIG = loadConfiguration();
const optionsNoOutput = {
  encoding: 'utf-8',
  stdio: ['ignore', 'ignore', 'ignore']
}

export async function runHealingTask( task, isInitialSync, publishDelta ) {
  async function updateDatabase(operation, updates, extraHeaders, publicationEndpoint, resultFileName, container) {
    console.log(`DEBUG: Starting ${operation.toLowerCase()} batch update`)
    await batchedUpdate(updates,
        PUBLICATION_GRAPH,
        operation,
        100,
        HEALING_PATCH_GRAPH_BATCH_SIZE,
        extraHeaders,
        publicationEndpoint
    );
    //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
    await createResultsContainer(task, updates, container, resultFileName);
  }

  try {
    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(EXPORT_CONFIG.export);

    let accumulatedDiffs;
    if (USE_FILE_DIFF) {
      accumulatedDiffs = {inserts: tmp.fileSync(), deletes: tmp.fileSync()};
    } else {
      accumulatedDiffs = {inserts: [], deletes: []};
    }

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

      console.log(`Calculating diffs for property ${property}, this may take a while`);
      let diffs;
      if (USE_FILE_DIFF) {
        let publicationGraphTriplesFile = arrayToFile(publicationGraphTriples, tmp.fileSync());
        let fileDiff = diffFiles(sourceTriples, publicationGraphTriplesFile);
        let newInserts = tmp.fileSync()
        execSync(`cat ${accumulatedDiffs.inserts.name} ${fileDiff.inserts.name} | tee ${newInserts.name}`, optionsNoOutput)
        fileDiff.inserts.removeCallback()
        accumulatedDiffs.inserts.removeCallback()
        accumulatedDiffs.inserts = newInserts

        let newDeletes = tmp.fileSync()
        execSync(`cat ${accumulatedDiffs.deletes.name} ${fileDiff.deletes.name} | tee ${newDeletes.name}`, optionsNoOutput)
        accumulatedDiffs.deletes.removeCallback()
        fileDiff.deletes.removeCallback()
        accumulatedDiffs.deletes = newDeletes

        publicationGraphTriplesFile.removeCallback();
        sourceTriples.removeCallback();
      } else {
        diffs = diffTriplesData(sourceTriples, publicationGraphTriples);
        accumulatedDiffs.deletes = [ ...accumulatedDiffs.deletes, ...diffs.deletes ];
        accumulatedDiffs.inserts = [ ...accumulatedDiffs.inserts, ...diffs.inserts ];
      }
    }

    let extraHeaders = { 'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE };
    if(isInitialSync){
      extraHeaders = { 'mu-call-scope-id': MU_CALL_SCOPE_ID_INITIAL_SYNC };
    }

    let publicationEndpoint = PUBLICATION_MU_AUTH_ENDPOINT;
    if(SKIP_MU_AUTH_INITIAL_SYNC && isInitialSync){
      console.warn(`Skipping mu-auth when injesting data, make sure you know what you're doing.`);
      publicationEndpoint = PUBLICATION_VIRTUOSO_ENDPOINT;
    }

    let fileDiffMaxArraySize = 1000000;
    if (USE_FILE_DIFF) {
      let deletes = [];
      console.log("DEBUG: getting data from deletes file")
      let rl = new Readlines(accumulatedDiffs.deletes.name)
      let line, part = 0;
      while ((line = rl.next())) {
        line = line.toString()
        deletes.push(JSON.parse(line).nTriple)
        // to make sure the deletes array does not explode in memory we push the update regularly
        if (deletes.length >= fileDiffMaxArraySize) {
          await updateDatabase("DELETE", deletes, extraHeaders, publicationEndpoint, `removed-triples-part-${part}.ttl`, REMOVAL_CONTAINER);
          deletes = [];
          part++;
        }
      }
      await updateDatabase("DELETE", deletes, extraHeaders, publicationEndpoint, `removed-triples-part-${part}.ttl`, REMOVAL_CONTAINER);
    } else {
      let deletes = accumulatedDiffs.deletes.map(t => t.nTriple);
      await updateDatabase("DELETE", deletes, extraHeaders, publicationEndpoint, 'removed-triples.ttl', REMOVAL_CONTAINER);
    }

    if (USE_FILE_DIFF) {
      let inserts = [];
      console.log("DEBUG: getting data from inserts file")
      let rl = new Readlines(accumulatedDiffs.inserts.name)
      let line, part = 0;
      while ((line = rl.next())) {
        line = line.toString()
        inserts.push(JSON.parse(line).nTriple)
        // to make sure the inserts array does not explode in memory we push the update regularly
        if (inserts.length >= fileDiffMaxArraySize) {
          await updateDatabase("INSERT", inserts, extraHeaders, publicationEndpoint, `inserted-triples-part-${part}.ttl`, INSERTION_CONTAINER);
          inserts = [];
          part++;
        }
      }
      await updateDatabase("INSERT", inserts, extraHeaders, publicationEndpoint, `inserted-triples-part-${part}.ttl`, INSERTION_CONTAINER);
    } else {
      let inserts = accumulatedDiffs.inserts.map(t => t.nTriple);
      await updateDatabase("INSERT", inserts, extraHeaders, publicationEndpoint, 'inserted-triples.ttl', INSERTION_CONTAINER);
    }

    console.log(`started at ${started}`);
    console.log(`ending at ${new Date()}`);
    if (publishDelta) {
      let deletes = [];
      let rl = new Readlines(accumulatedDiffs.deletes.name);
      let line;
      while ((line = rl.next())) {
        line = line.toString()
        deletes.push(JSON.parse(line).nTriple)
        // to make sure the deletes array does not explode in memory we push the update regularly
        if (deletes.length >= fileDiffMaxArraySize) {
          await publishDeltaFiles({deletes: deletes, inserts: []})
          deletes = [];
        }
      }
      let inserts = [];
      rl = new Readlines(accumulatedDiffs.inserts.name);
      line = "";
      while ((line = rl.next())) {
        line = line.toString()
        inserts.push(JSON.parse(line).nTriple)
        // to make sure the inserts array does not explode in memory we push the update regularly
        if (inserts.length >= fileDiffMaxArraySize) {
          await publishDeltaFiles({inserts: inserts, deletes: []})
          inserts = [];
        }
      }

      // push the remaining inserts and deletes
      await publishDeltaFiles({deletes: deletes, inserts: inserts});
    }
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
  let sourceTriples;
  if (USE_FILE_DIFF) {
    sourceTriples = tmp.fileSync();
  } else {
    sourceTriples = []
  }
  for(const config of propertyMap[property]){
    let scopedSourceTriples = await getScopedSourceTriples(config,
                                                             property,
                                                             conceptSchemeUri,
                                                             PUBLICATION_GRAPH,
                                                             EXPORT_CONFIG);
    console.log(`DEBUG: number of source triples: ${scopedSourceTriples.length}`)

    if (USE_FILE_DIFF) {
      scopedSourceTriples = arrayToFile(scopedSourceTriples, tmp.fileSync());
      const diffs = diffFiles(scopedSourceTriples, sourceTriples);
      let newSourceTriples = tmp.fileSync();
      execSync(`cat ${sourceTriples.name} ${diffs.name} | tee ${newSourceTriples.name}`, optionsNoOutput)
      sourceTriples.removeCallback();
      sourceTriples = newSourceTriples;
      scopedSourceTriples.removeCallback();
    } else {
      const diffs = diffTriplesData(scopedSourceTriples, sourceTriples);
      console.log(`DEBUG: FILE BASED DIFF, number of inserts: ${diffs.inserts.length} | number of deletes: ${diffs.deletes.length}`)
      sourceTriples = [ ...sourceTriples, ...diffs.inserts ];
    }
  }
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
  const endpoint = USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS ? PUBLICATION_VIRTUOSO_ENDPOINT : PUBLICATION_MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromPublicationGraph, {}, { sparqlEndpoint: endpoint, mayRetry: true });
  return reformatQueryResult(result);
}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
async function getScopedSourceTriples( config, property, conceptSchemeUri, publicationGraph, exportConfig ){
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

  //Note: this might explose memory, but now, a paginated fetch is extremely slow. (because sorting)
  const endpoint = USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromDatabase, {}, { sparqlEndpoint: endpoint, mayRetry: true });
  return reformatQueryResult(result);
}

function arrayToFile(array, file){
  let fd = file.fd;
  for (let i=0; i<array.length; ++i){
    fs.writeSync(fd, JSON.stringify(array[i]) + "\n")
  }
  return file
}
// read the file and parse each line to an Object, the opposite of the above function
function lines(filename) {
  let retval = []
  let rl = new Readlines(filename)
  let line
  while ((line = rl.next())) {
    line = line.toString()
    retval.push(JSON.parse(line))
  }
  return retval;
}
function diffFiles(targetFile, sourceFile, S="50%", T="/tmp"){
  // Note: the S and T parameters can be used to tweak the memory usage of the sort command
  console.log(`DEBUG: DIFFING FILE BASED`);

  let sorted1 = tmp.fileSync();
  let sorted2 = tmp.fileSync();

  execSync(`sort ${targetFile.name} -S ${S} -T ${T} -o ${sorted1.name}`, optionsNoOutput)
  execSync(`sort ${sourceFile.name} -S ${S} -T ${T} -o ${sorted2.name}`, optionsNoOutput)

  let output1 = tmp.fileSync();
  let output2 = tmp.fileSync();

  execSync(`comm -23 ${sorted1.name} ${sorted2.name} | tee ${output1.name}`, optionsNoOutput)
  execSync(`comm -13 ${sorted1.name} ${sorted2.name} | tee ${output2.name}`, optionsNoOutput)

  sorted1.removeCallback();
  sorted2.removeCallback();

  return {
    inserts: output1,
    deletes: output2
  }
}


function diffTriplesData(target, source) {
  //Note: this only works correctly if triples have same lexical notation.
  //So think about it, when copy pasting :-)

  let diff = { inserts: [], deletes: [] };
  if (target.length === 0) {
    diff.deletes = source;
  } else if (source.length === 0) {
    diff.inserts = target;
  } else if (USE_FILE_DIFF) {
    console.log(`DEBUG: FILE BASED DIFF, target size is ${target.length}, source size is ${source.length}`)
    // only do the file-based diff when the dataset is large, since otherwise the overhead is too much
    let targetFile = arrayToFile(target, tmp.fileSync())
    let sourceFile = arrayToFile(source, tmp.fileSync())
    let fileDiff = diffFiles(targetFile, sourceFile)
    console.log(`DEBUG: FILE BASED DIFF, calculating inserts and deletes from files`)
    diff = {
      inserts: lines(fileDiff.inserts.name),
      deletes: lines(fileDiff.deletes.name)
    }
    fileDiff.inserts.removeCallback()
    fileDiff.deletes.removeCallback()
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

function reformatQueryResult( result ){
  let triplesData = [];

  if(result.results && result.results.bindings && result.results.bindings.length){
    const triples = result.results.bindings;
    triplesData = triples.map(t => {
      return {
        nTriple: serializeTriple(t)
        // originalFormat: t
      };
    });
  }

  return triplesData;
}
