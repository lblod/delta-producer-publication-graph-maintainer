import { publishDeltaFiles } from "../../files-publisher/main";
import { chunk } from 'lodash';

import {
  groupPathToConceptSchemePerProperty,
  getScopedSourceTriples,
  getScopedPublicationTriples,
  updateDatabase
}  from './utils';

import {
  DELTA_CHUNK_SIZE,
  PUBLICATION_MU_AUTH_ENDPOINT,
  PUBLICATION_VIRTUOSO_ENDPOINT,
} from "../../env-config";

/*
 * This function runs the healing task in the 'classic' way; i.e with DB and in memory diffing
 * Although very similar to file-based-healing.js, the boilerplate is different
 */
export async function runHealingTask(serviceConfig, serviceExportConfig, task, isInitialSync, publishDelta) {

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

      const sourceTriples = await getTriples(serviceConfig,
                                             property, propertyMap,
                                             conceptSchemeUri,
                                             getScopedSourceTriples);

      const publicationGraphTriples = await getTriples(serviceConfig,
                                                       property, propertyMap,
                                                       conceptSchemeUri,
                                                       getScopedPublicationTriples);

      console.log(`Calculating diffs for property ${property}, this may take a while`);
      let diffs = diffTriplesData(serviceConfig, sourceTriples, publicationGraphTriples);

      accumulatedDiffs.deletes = [ ...accumulatedDiffs.deletes, ...diffs.deletes ];
      accumulatedDiffs.inserts = [ ...accumulatedDiffs.inserts, ...diffs.inserts ];

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

    let deletes = accumulatedDiffs.deletes.map(t => t.nTriple);
    await updateDatabase(serviceConfig, task, "DELETE", deletes, extraHeaders, publicationEndpoint, 'removed-triples.ttl', serviceConfig.removalContainer);

    let inserts = accumulatedDiffs.inserts.map(t => t.nTriple);
    await updateDatabase(serviceConfig, task, "INSERT", inserts, extraHeaders, publicationEndpoint, 'inserted-triples.ttl', serviceConfig.insertionContainer);

    console.log(`Started at ${started}`);
    console.log(`Ended at ${new Date()}`);

    if (publishDelta) {
      await pushToDeltaFiles(serviceConfig, "DELETE", accumulatedDiffs.deletes, fileDiffMaxArraySize);
      await pushToDeltaFiles(serviceConfig, "INSERT", accumulatedDiffs.inserts, fileDiffMaxArraySize);
    }
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
  let sourceTriples = [];

  for(const config of propertyMap[property]){
    let scopedSourceTriples = await getTriplesCall(serviceConfig,
                                                           config,
                                                           property,
                                                           serviceConfig.publicationGraph,
                                                           conceptSchemeUri);
    console.log(`Number of source triples: ${scopedSourceTriples.length}`);

    const diffs = diffTriplesData(serviceConfig, scopedSourceTriples, sourceTriples);
    sourceTriples = [ ...sourceTriples, ...diffs.inserts ];
  }

  return sourceTriples;
}

function diffTriplesData(serviceConfig, target, source) {
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

async function pushToDeltaFiles(serviceConfig, operation, triples, fileDiffMaxArraySize) {
  let chunkedTriples = chunk(triples, fileDiffMaxArraySize);
  // To make sure the array does not explode in memory we push the update regularly
  for(const triplesBatch of chunkedTriples) {
    const data = operation == "DELETE" ?
          { deletes: triplesBatch, inserts: []} : {deletes: [], inserts: triplesBatch};
    await publishDeltaFiles(serviceConfig, data);
  }
}
