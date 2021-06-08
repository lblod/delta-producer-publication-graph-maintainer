import { uuid, sparqlEscapeUri, sparqlEscapeString } from 'mu';
import { STATUS_BUSY,
         STATUS_FAILED,
         STATUS_SUCCESS,
         PUBLICATION_GRAPH,
         INSERTION_CONTAINER,
         REMOVAL_CONTAINER,
         REPORTING_FILES_GRAPH
       } from '../env-config';
import {  updateTaskStatus, appendTaskError, appendTaskResultFile } from '../lib/task';
import { sparqlEscapePredicate, batchedQuery, batchedUpdate, serializeTriple } from '../lib/utils';
import { writeTtlFile } from  '../lib/file-helpers';
import { uniq } from 'lodash';

const EXPORT_CONFIG = require('/config/export.json');

export async function runHealingTask( task ){
  try {
    await updateTaskStatus(task, STATUS_BUSY);

    const conceptSchemeUri = EXPORT_CONFIG.conceptScheme;
    const started = new Date();

    console.log(`starting at ${started}`);

    const propertyMap = groupPathToConceptSchemePerProperty(EXPORT_CONFIG.export);

    let accumulatedDiffs = { additions: [], removals: [] };

    //Some explanation:
    // The triples to push to the publication graph should be equal to
    // - all triples whose ?s link to a the conceptscheme (through pathToConceptScheme) and
    // - whose ?p match the properties defined in the EXPORT_CONFIG and
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
      const publicationGraphTriples = await getPublicationdTriples(property, PUBLICATION_GRAPH);
      const diffs = diffNTriples(sourceTriples, publicationGraphTriples);

      accumulatedDiffs.removals = [ ...accumulatedDiffs.removals, ...diffs.removals ];
      accumulatedDiffs.additions = [ ...accumulatedDiffs.additions, ...diffs.additions ];

    }

    if(accumulatedDiffs.removals.length){
      await batchedUpdate(accumulatedDiffs.removals, PUBLICATION_GRAPH, 'DELETE', 500);
      //We will keep two containers to attach to the task, so we have better reporting on what has been corrected
      await createResultsContainer(task, accumulatedDiffs.removals, REMOVAL_CONTAINER, 'removed-triples.ttl');
    }

    if(accumulatedDiffs.additions.length){
      await batchedUpdate(accumulatedDiffs.additions, PUBLICATION_GRAPH, 'INSERT', 500);
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
    const extendedProperties = [...configEntry.properties, 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'];
    for(const property of extendedProperties){
      if(result[property]){
        result[property].push(
          {
            pathToConceptScheme: configEntry.pathToConceptScheme,
            graphsFilter: configEntry.graphsFilter
          }
        );
      }
      else {
        result[property] =
        [
          {
            pathToConceptScheme: configEntry.pathToConceptScheme,
            graphsFilter: configEntry.graphsFilter
          }
        ];
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
    const scopedSourceTriples = await getScopedSourceTriples(config.pathToConceptScheme,
                                                             config.graphsFilter || [],
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
async function getPublicationdTriples(property, publicationGraph){
  const selectFromPublicationGraph = `
    SELECT DISTINCT ?subject ?predicate ?object WHERE {
      BIND(${sparqlEscapeUri(property)} as ?predicate)

      GRAPH ${sparqlEscapeUri(publicationGraph)}{
        ?subject ?predicate ?object.
      }
     }
  `;

  const publicationResult = await batchedQuery(selectFromPublicationGraph, 1000);
  const publicationNTriples = publicationResult.map(t => serializeTriple(t));

  return publicationNTriples;

}

/*
 * Gets the source triples for a property and a pathToConceptScheme from the database,
 * for all graphs except the ones exclusively residing in the publication graph
 */
async function getScopedSourceTriples( pathToConceptScheme, graphsFilter, property, conceptSchemeUri, publicationGraph, exportConfig ){

  const predicatePath = pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');

  let selectFromDatabase = '';

  if(graphsFilter.length) {

    const graphsFilterStr = graphsFilter
          .map(g => `regex(str(?g), ${sparqlEscapeString(g)})`)
          .join(' || ');

    selectFromDatabase = `
      SELECT DISTINCT ?subject ?predicate ?object WHERE {
        BIND(${sparqlEscapeUri(property)} as ?predicate)
        GRAPH ?g {
          ?subject ?predicate ?object.
        }
        ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.

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
        GRAPH ?g {
          ?subject ?predicate ?object.
        }
        ?subject ${predicatePath} ${sparqlEscapeUri(conceptSchemeUri)}.
        FILTER(?g NOT IN (${sparqlEscapeUri(publicationGraph)}))
       }
    `;
  }

  //Note: even if the ordering might be slow, we need ordered triples to compare against on file
  const sourceResult = await batchedQuery(selectFromDatabase, 1000);
  const sourceNTriples = sourceResult.map(t => serializeTriple(t));

  return sourceNTriples;
}

function diffNTriples(target, source) {
  //Note: this only works correctly if triples have same lexical notation.
  //So think about it, when copy pasting :-)
  const diff = { additions: [], removals: [] };
  const targetString = target.join('\n');
  const sourceString = source.join('\n');

  diff.additions = target.filter(nt => sourceString.indexOf(nt) < 0);
  diff.removals = source.filter(nt => targetString.indexOf(nt) < 0);

  return diff;
}
