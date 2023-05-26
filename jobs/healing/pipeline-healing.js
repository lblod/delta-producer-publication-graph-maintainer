import { querySudo as query } from '@lblod/mu-auth-sudo';
import { uniq } from 'lodash';
import { sparqlEscapeString, sparqlEscapeUri, uuid } from 'mu';
import { writeTtlFile } from '../../lib/file-helpers';
import { appendTaskResultFile } from '../../lib/task';
import {batchedUpdate, serializeTriple, sparqlEscapePredicate} from '../../lib/utils';
import {
  MU_AUTH_ENDPOINT,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
  VIRTUOSO_ENDPOINT
} from "../../env-config";
import {publishDeltaFiles} from "../../files-publisher/main";


const optionsNoOutput = {
  encoding: 'utf-8',
  stdio: ['ignore', 'ignore', 'ignore'],
  shell: '/bin/bash'
}

export async function runHealingTask(service_config, service_export_config, task, isInitialSync, publishDelta ) {
  async function updateDatabase(service_config, operation, updates, extraHeaders, publicationEndpoint, resultFileName, container) {
    console.log(`DEBUG: Starting ${operation.toLowerCase()} batch update`)
    await batchedUpdate(updates,
        service_config.publicationGraph,
        operation,
        100,
        service_config.healingPatchGraphBatchSize,
        extraHeaders,
        publicationEndpoint
    );
    //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
    await createResultsContainer(task, updates, container, resultFileName);
  }

  try {
    const conceptSchemeUri = service_export_config.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(service_export_config.export);

    let accumulatedDiffs;
    if (service_config.useFileDiff) {
      accumulatedDiffs = {inserts: tmp.fileSync(), deletes: tmp.fileSync()};
    } else {
      accumulatedDiffs = {inserts: [], deletes: []};
    }

    //Some explanation:
    // The triples to push to the publication graph should be equal to
    // - all triples whose ?s link to the concept scheme (through pathToConceptScheme)
    //   (note if no path defined, then this condition returns true) AND
    // - whose ?p match the properties defined in the service_export_config AND
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

      const sourceTriples = await getSourceTriples(service_config, service_export_config, property, propertyMap, conceptSchemeUri);
      const publicationGraphTriples = await getPublicationTriples(service_config, property, service_config.publicationGraph);

      console.log(`Calculating diffs for property ${property}, this may take a while`);
      if (service_config.useFileDiff) {
        let fileDiff = diffFiles(sourceTriples, publicationGraphTriples);
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

        sourceTriples.removeCallback();
      } else {
        let diffs = diffTriplesData(sourceTriples, publicationGraphTriples);
        accumulatedDiffs.deletes = [ ...accumulatedDiffs.deletes, ...diffs.deletes ];
        accumulatedDiffs.inserts = [ ...accumulatedDiffs.inserts, ...diffs.inserts ];
      }
    }

    let extraHeaders = { 'mu-call-scope-id': service_config.muCallScopeIdPublicationGraphMaintenance };
    if(isInitialSync){
      extraHeaders = { 'mu-call-scope-id': service_config.muCallScopeIdInitialSync };
    }

    let publicationEndpoint = PUBLICATION_MU_AUTH_ENDPOINT;
    if(service_config.skipMuAuthInitialSync && isInitialSync){
      console.warn(`Skipping mu-auth when injesting data, make sure you know what you're doing.`);
      publicationEndpoint = PUBLICATION_VIRTUOSO_ENDPOINT;
    }

    let fileDiffMaxArraySize = 1000000;
    if (service_config.useFileDiff) {
      let deletes = [];
      console.log("DEBUG: getting data from deletes file")
      let rl = new Readlines(accumulatedDiffs.deletes.name)
      let line, part = 0;
      while ((line = rl.next())) {
        line = line.toString()
        deletes.push(JSON.parse(line).nTriple)
        // to make sure the deletes array does not explode in memory we push the update regularly
        if (deletes.length >= fileDiffMaxArraySize) {
          await updateDatabase(service_config, "DELETE", deletes, extraHeaders, publicationEndpoint, `removed-triples-part-${part}.ttl`, service_config.removalContainer);
          deletes = [];
          part++;
        }
      }
      await updateDatabase("DELETE", deletes, extraHeaders, publicationEndpoint, `removed-triples-part-${part}.ttl`, REMOVAL_CONTAINER);
    } else {
      let deletes = accumulatedDiffs.deletes.map(t => t.nTriple);
      await updateDatabase("DELETE", deletes, extraHeaders, publicationEndpoint, 'removed-triples.ttl', REMOVAL_CONTAINER);
    }

    if (service_config.useFileDiff) {
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
          await publishDeltaFiles(service_config, {deletes: deletes, inserts: []})
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
          await publishDeltaFiles(service_config, {inserts: inserts, deletes: []})
          inserts = [];
        }
      }

      // push the remaining inserts and deletes
      await publishDeltaFiles(service_config, {deletes: deletes, inserts: inserts});
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

async function createResultsContainer(service_config, task, nTriples, subject, fileName ){
  const fileContainer = { id: uuid(), subject };
  fileContainer.uri = `http://data.lblod.info/id/dataContainers/${fileContainer.id}`;
  const turtleFile = await writeTtlFile(service_config.reportingFilesGraph || task.graph, nTriples.join('\n'), fileName);
  await appendTaskResultFile(task, fileContainer, turtleFile);
}

/*
 * Gets the triples for a property, which are considered 'Ground Truth'
 */
async function getSourceTriples(service_config, service_export_config, property, propertyMap, conceptSchemeUri ){
  let sourceTriples = [];
  for(const config of propertyMap[property]){
    let scopedSourceTriples = await getScopedSourceTriples(service_config,
                                                             config,
                                                             property,
                                                             conceptSchemeUri,
                                                             service_config.publicationGraph,
                                                             service_export_config);

    if (service_config.useFileDiff) {
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
async function getPublicationTriples(service_config, property, publicationGraph){
  console.log(`DEBUG: Publication triples using file? ${service_config.useFileDiff}`)
  if (service_config.useFileDiff) {
    // when using file-based diff, query the database using isql to a file
    let username = "dba", password = "dba";
    // function to create serialized triple from the subject, predicate, object and possibly its type and language
    function generateTripleFromParts(parts){
      let subject = parts[0]
      let predicate = parts[1]
      let object = parts[2]
      let type = parts[3]
      let lang = parts[4]

      let objectType = 'uri';
      if (type === "http://www.w3.org/2001/XMLSchema#string") {
        objectType = "literal"
      } else if (type){
        objectType = "typed-literal"
      }
      return {
        nTriple: serializeTriple({
          subject: {
            type: "uri", value: subject
          },
          predicate: {
            type: "uri", value: predicate
          },
          object: {
            type: objectType, datatype: type, lang: lang, value: object
          }
        })
      }
    }
    let inputFile = tmp.fileSync();
    execSync(`isql Virtuoso ${username} ${password} <<< "sparql select ?blank ?s ?p ?o ?o_type ?o_lang where { graph ${sparqlEscapeUri(publicationGraph)} {?s ?p ?o.} bind(${sparqlEscapeUri(property)} as ?p) bind(datatype(?o) as ?o_type) bind(lang(?o) as ?o_lang) bind('' as ?blank)}" -b -L1000000 -x8 | tee ${inputFile.name}`, optionsNoOutput)
    let rl = new Readlines(inputFile.name)
    let outputFile = tmp.fileSync();
    let parts = `${rl.next()}`.split("\x08");
    let line, tripleParts = parts.slice(1), triples = [];
    while((line = rl.next())){
      line = `${line}`
      let parts = line.split("\x08")
      if (line[0] === "\x08" && tripleParts.length === 5) {
        triples.push(generateTripleFromParts(tripleParts, outputFile))
        tripleParts = parts.slice(1)
      } else {
        tripleParts[tripleParts.length-1] += parts[0]
        if (parts.length > 1){
          tripleParts = [...tripleParts, ...parts.slice(1)]
        }
      }
      // only write to the outputFile with a bunch of triples
      let numberOfTriplesInMemory = 1000;
      if (triples.length === numberOfTriplesInMemory){
        arrayToFile(triples, outputFile);
        triples = [];
      }
    }
    // write the remaining triples to the outputFile
    arrayToFile(triples, outputFile);
    inputFile.removeCallback()
    return outputFile;
  } else {
    const selectFromPublicationGraph = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      GRAPH ${sparqlEscapeUri(publicationGraph)}{
        ?subject ?predicate ?object.
      }
     }
  `;

  //Note: this might explode memory, but now, a paginated fetch is extremely slow. (because sorting)
  const endpoint = service_config.useVirtuosoForExpensiveSelects ? PUBLICATION_VIRTUOSO_ENDPOINT : PUBLICATION_MU_AUTH_ENDPOINT;
  console.log(`Hitting database ${endpoint} with expensive query`);
  const result = await query(selectFromPublicationGraph, {}, { sparqlEndpoint: endpoint, mayRetry: true });
  return reformatQueryResult(result);
  }
}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
async function getScopedSourceTriples(service_config, config, property, conceptSchemeUri, publicationGraph, exportConfig ){
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
  const endpoint = service_config.useVirtuosoForExpensiveSelects ? VIRTUOSO_ENDPOINT : MU_AUTH_ENDPOINT;
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
