import * as tmp from 'tmp';
import * as Readlines from '@lazy-node/readlines';
import { batchedUpdate } from '../../lib/utils';
import { publishDeltaFiles } from "../../files-publisher/main";

import {
  groupPathToConceptSchemePerProperty,
  getScopedSourceTriples,
  getScopedPublicationTriples,
  updateDatabase
}  from './utils';

import {
  diffFiles,
  mergeFiles,
  arrayToFile,
  lines
} from './file-utils';

import {
  DELTA_CHUNK_SIZE,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
} from "../../env-config";

export async function runHealingTask(serviceConfig, serviceExportConfig, task, isInitialSync, publishDelta) {

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
                                             property, propertyMap,
                                             conceptSchemeUri,
                                             getScopedSourceTriples);

      const publicationGraphTriples = await getTriples(serviceConfig,
                                                       property, propertyMap,
                                                       conceptSchemeUri,
                                                       getScopedPublicationTriples);

      console.log(`Calculating diffs for property ${property}, this may take a while`);
      if (serviceConfig.useFileDiff) {

        let fileDiff = diffFiles(sourceTriples, publicationGraphTriples);

        accumulatedDiffs.inserts = mergeFiles(accumulatedDiffs.inserts, fileDiff.inserts, true);
        accumulatedDiffs.deletes = mergeFiles(accumulatedDiffs.deletes, fileDiff.deletes, true);

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

/*
 * Gets the triples for a property
 */
async function getTriples(serviceConfig, property, propertyMap, conceptSchemeUri, getTriplesCall ){
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

      sourceTriples = mergeFiles(sourceTriples, diffs.inserts, true);

      diffs.inserts.removeCallback();
      diffs.deletes.removeCallback();
      scopedSourceTriplesFile.removeCallback();
    } else {
      const diffs = diffTriplesData(serviceConfig, scopedSourceTriples, sourceTriples);
      sourceTriples = [ ...sourceTriples, ...diffs.inserts ];
    }
  }

  return sourceTriples;
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
