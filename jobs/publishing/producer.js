import { sparqlEscapeString, sparqlEscapeUri } from 'mu';
import { querySudo as query } from '@lblod/mu-auth-sudo';
import { uniq } from 'lodash';
import {
  isInverse,
  sparqlEscapePredicate,
  normalizePredicate,
  serializeTriple,
  isSamePath
} from '../../lib/utils';
import {
  LOG_DELTA_REWRITE,
  PUBLICATION_MU_AUTH_ENDPOINT
} from "../../env-config";

// TODO add support for a prefix map in the export configuration
//      preprocess the imported config by resolving all prefixed URIs with full URIs

/**
 * Rewriting the incoming delta message to a delta message relevant for the conceptScheme export
 */
export async function produceDelta(service_config, service_export_config, delta) {
  const updatedDeltas = [];

  for (let changeSet of delta) {
    const updatedChangeSet = {inserts: [], deletes: []};

    const typeCache = await buildTypeCache(service_config, service_export_config, changeSet);

    if (LOG_DELTA_REWRITE)
      console.log(`Rewriting inserted changeSet containing ${changeSet.inserts.length} triples`);
    const inserts = await rewriteInsertedChangeset(service_config, service_export_config, changeSet.inserts, typeCache);
    updatedChangeSet.inserts.push(...inserts);

    if (LOG_DELTA_REWRITE)
      console.log(`Rewriting deleted changeSet containing ${changeSet.deletes.length} triples`);
    const deletedSubjects = changeSet.deletes.map(t => t.subject.value);
    const deletes = await rewriteDeletedChangeset(service_config, service_export_config, deletedSubjects, typeCache);
    updatedChangeSet.deletes.push(...deletes);

    if (updatedChangeSet.inserts.length || updatedChangeSet.deletes.length)
      updatedDeltas.push(updatedChangeSet);
  }

  // TODO optimization: remove sequential changesets that insert/delete exactly the same triples (or vice versa)

  return updatedDeltas;
}

/**
 * Build a mapping of URIs to their export config for each subject/object URI in a changeset
 * based on the rdf:type of the URI. The mapping will be used as an in-memory cache.
 * There may be multiple entries in the mapping for a specific URI. Either, the URI has
 * multiple rdf:type or there are multiple export configurations for one rdf:type.
 *
 * For deletions it's important to note that the deleted data is not available anymore
 * in the store since the data has been deleted. That's why the rdf:type for a URI is retrieved
 * from the triple store as well as from the delta changeset.
 *
 * TODO: verify if the publicationGraph could be a more robust replacement. I.e. in case of deletion,
 * both delta and publicationGraph may have their use. Both refer to outdated information.
 * -> why using the publicationGraph could be considered better: with delta, it might be that the changeset is
 * not atomic, meaning delete statements [<foo> <bar> <baz>, <foo> a <Type>] might be spread over 2 deltas,
 * while the type information is not present anymore in the source graph. In that sense the type publication might be outdated.
 *
 * Building the cache is purely based on rdf:type and does not take the path from
 * a resource to the export concept scheme into account.
 *
 * @param service_config the configuration to be used
 * @param object changeSet Delta changeset including `inserts` and `deletes`
 * @return Array Array of objects like { uri, config }
 */
async function buildTypeCache(service_config, service_export_config, changeSet) {
  const cache = [];

  const triples = [...changeSet.inserts, ...changeSet.deletes];
  const subjects = triples.map(t => t.subject.value);
  const objects = triples.filter(t => t.object.type == 'uri').map(t => t.object.value);
  const uris = uniq([...subjects, ...objects]);
  if (LOG_DELTA_REWRITE)
    console.log(`Building type cache for ${uris.length} URIs based on types found in the store and the changeset.`);

  for (let uri of uris) {
    const resultFromSourceStore = await query(`
      SELECT DISTINCT ?type WHERE {
        GRAPH ?g {
          ${sparqlEscapeUri(uri)} a ?type.
        }
      }
    `);
    const typesFromStore = resultFromSourceStore.results.bindings.map(b => b['type'].value);

    const resultFromPublicationStore = await query(`
      SELECT DISTINCT ?type WHERE {
        GRAPH ${sparqlEscapeUri(service_config.publicationGraph)} {
          ${sparqlEscapeUri(uri)} a ?type.
        }
      }
    `, {}, { sparqlEndpoint: PUBLICATION_MU_AUTH_ENDPOINT, mayRetry: true });
    const typesFromPublicationStore = resultFromPublicationStore.results.bindings.map(b => b['type'].value);

    const typesFromChangeset = triples.filter(
        t => t.subject.value == uri && t.predicate.value == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
          .map(t => t.object.value);


    const types = uniq([...typesFromStore, ...typesFromChangeset, ...typesFromPublicationStore]);
    if (LOG_DELTA_REWRITE)
      console.log(`Found ${types.length} distinct types for URI <${uri}>.`);

    for (let type of types) {
      const typeConfigs = service_export_config.export.filter(t => t.type == type);
      if (typeConfigs.length) {
        if (LOG_DELTA_REWRITE)
          console.log(`rdf:type <${type}> configured for export.`);
        const cacheEntries = typeConfigs.map(config => {
          return {uri, config};
        });
        cache.push(...cacheEntries);
      } else if (LOG_DELTA_REWRITE) {
        console.log(`rdf:type <${type}> is not configured for export and will be ignored.`);
      }
    }
  }

  return cache;
}

/**
 * Rewrite the received triples to insert to an insert changeset relevant for the export of conceptSchemes.
 *
 * Insertion of 1 triple may lead to a bunch of resources to be exported,
 * because the newly inserted triple completes the path to the export concept scheme.
 *
 * E.g. Linking a mandatee to a mandate may cause the export of the mandate, but as well
 *      the export of the person, the person's birthdate etc.
 *
 * High-level description of the function:
 * The function first walks over the received insert changeset
 * and checks for each triple whether it should be exported based
 * on the subject's type and the export configuration.
 * Next, for each inserted relation triple (ie. object is a URI, not a literal)
 * it checks whether the subject/object needs to be added as an additional resource
 * based on the predicate and the export configuration.
 * This enrichment is checked recursively for any newly added relation triple as well.
 */
async function rewriteInsertedChangeset(service_config, service_export_config, changeSet, typeCache) {
  const triplesToInsert = [];
  const processedResources = []; // tmp cache of recursively added resources

  // For each triple of the changeset, check if it is relevant for the export.
  // Ie. the subject has a path to the export concept-scheme and the predicate is configured to be exported
  for (let triple of changeSet) {
    const uri = triple.subject.value;
    const exportConfigurations = typeCache.filter(e => e.uri === uri).map(e => e.config);
    if (exportConfigurations.length) {
      for (let config of exportConfigurations) {
        const isInScope = await isInScopeOfConfiguration(service_config, service_export_config, uri, config);
        if (isInScope) {
          // We don't check if the resource has already been processed,
          // different configuration could contain different predicates
          if (config.additionalFilter) {
            processedResources.push(uri);
            if (LOG_DELTA_REWRITE)
              console.log(`Additional Filters found, enriching insert changeset with export of resource <${uri}>.`);
            const resourceExport = await exportResource(service_config, uri, config);
            triplesToInsert.push(...resourceExport);
          } else if (isConfiguredForExport(triple, config)) {
            if (LOG_DELTA_REWRITE)
              console.log(`Triple ${serializeTriple(triple)} copied to insert changeset for export.`);
            triplesToInsert.push(triple);
          } else {
            if (LOG_DELTA_REWRITE)
              console.log(`Triple ${serializeTriple(triple)} not relevant for export and will be ignored.`);
          }
        } else {
          if (LOG_DELTA_REWRITE)
            console.log(`No path to export concept scheme found for subject <${uri}>.`);
        }
      }
    } else if (LOG_DELTA_REWRITE) {
      console.log(
          `Triple ${serializeTriple(triple)} has a rdf:type that is not configured for export and will be ignored.`);
    }
  }

  const enrichments = await enrichInsertedChangeset(service_config, service_export_config, triplesToInsert, typeCache, processedResources);
  if (LOG_DELTA_REWRITE) {
    console.log(`Checked ${processedResources.length} additional resources to insert.`);
    console.log(`Adding ${enrichments.length} triples to insert to the changeset.`);
  }
  triplesToInsert.push(...enrichments);

  return triplesToInsert;
}

/**
 * Enrich the insert changeset with resources that become
 * relevant for the export based on the triples in the original insert changeset.
 * Ie. 'deeper' resources that now also have a complete path to the export concept scheme.
 * Eg. insertion of a mandatee may cause the insertion of the person resource as well.
 *
 * The function is recursively applied to insert new related resources for the resources
 * that have just been added.
 * Eg. addition of the person in the previous example may cause the insertion
 * of the person's birthdate as well.
 */
async function enrichInsertedChangeset(service_config, service_export_config, changeSet, typeCache, processedResources) {
  const impactedResources = getImpactedResources(service_config, service_export_config, changeSet, typeCache);

  const triplesToInsert = [];
  for (let {uri, config} of impactedResources) {
    if (!processedResources.includes(uri)) {
      processedResources.push(uri); // make sure to handle each resource only once
      const isInScope = await isInScopeOfConfiguration(service_config, service_export_config, uri, config);
      if (isInScope) {
        if (LOG_DELTA_REWRITE)
          console.log(`Enriching insert changeset with export of resource <${uri}>.`);
        const resourceExport = await exportResource(service_config, uri, config);
        triplesToInsert.push(...resourceExport);

        if (LOG_DELTA_REWRITE)
          console.log(
              `Recursively checking for enrichments based on the newly inserted triples for resource <${uri}>.`);
        const recursiveTypeCache = await buildTypeCache(service_config, service_export_config, {inserts: resourceExport, deletes: []});
        const recursiveTriples = await enrichInsertedChangeset(service_config, service_export_config, resourceExport, recursiveTypeCache, processedResources);
        triplesToInsert.push(...recursiveTriples);
      } else if (LOG_DELTA_REWRITE) {
        console.log(`Resource <${uri}> doesn't have a path to the export concept scheme. Ignoring this resource.`);
      }
    } else {
      if (LOG_DELTA_REWRITE)
        console.log(`Resource <${uri}> already added to export for given config. Ignoring now.`);
    }
  }

  return triplesToInsert;
}

/**
 * Calculates the triples to delete from the publication graph.
 * Based on the configuration, it calculates the diffs between what is published and what should be removed.
 * There is an extra complication, when working with conceptScheme based configurations.
 * Then it should calculate a potential 'CASCADE DELETE', based on the conceptscheme.
 *
 * For instance, deleting a triple relating to a mandaat:Mandataris might also require deleting the associated person,
 * unless that person is linked to another mandaat:Mandataris.
 *
 * @param {Object} service_config - Configuration object for the service containing the config for the specific delta stream.
 * @param {Object} service_export_config - Export configuration of the resources to export themselves.
 * @param {Array<string>} subjectUris - URIs of the subjects that are potentially affected by the changeset deletions.
 * @param {Array<Object>} typeCache - A cache of type configurations, each containing URIs and their respective export settings.
 * @param {boolean} [recursivelyCalled=false] - Indicates whether this function is being called recursively to handle cascading deletions.
 *                                              -  That's mereley for logging purposes...
 *
 * @returns {Promise<Array<Object>>} The list of triples marked for delete.
 *
 */
async function rewriteDeletedChangeset(service_config, service_export_config, subjectUris, typeCache, recursivelyCalled = false) {
  const triplesToDelete = [];
  const subjectsForPotentialCascadeRemoval = [];

  for (let subjectUri of subjectUris) {
    const allSourceData = [];
    const allPublishedData = [];

    const exportConfigurations = typeCache.filter(e => e.uri === subjectUri).map(e => e.config);

    if (exportConfigurations.length) {
      for (let config of exportConfigurations) {
        const sourceData = await exportResource(service_config, subjectUri, config);
        const publishedData = await exportResource(service_config, subjectUri, config, true);

        allSourceData.push(...sourceData);
        allPublishedData.push(...publishedData);
      }
    }

    const dataToRemove = diffDeltas(allSourceData, allPublishedData).onlyInTarget;
    triplesToDelete.push(...dataToRemove);

    // Determine if additional subjects should be removed based on the removal logic outlined in the documentation.
    const normalRelations = dataToRemove
          .filter(t => t.subject.value == subjectUri && t.object.type == 'uri')
          .map(t => t.object.value);
    subjectsForPotentialCascadeRemoval.push(...normalRelations);

    const inverseRelations = dataToRemove
          .filter(t => t.subject.value != subjectUri)
          .map(t => t.subject.value);
    subjectsForPotentialCascadeRemoval.push(...inverseRelations);
  }

  if (LOG_DELTA_REWRITE) {
    if(recursivelyCalled) {
      console.log(`Calculating triples to be potentially removed in cascade.`);
    }
    console.log(`
      The following subjects were marked as deletion: ${subjectUris.join('\n')}.
      Resulted in triples marked as delete in the publicationGraph: ${triplesToDelete.map(t => serializeTriple(t)).join('\n')}.
    `);
  }

  if(subjectsForPotentialCascadeRemoval.length) {
    // Note:  it's a bit abusing the interface of this method. Subject to refactor.
    if (LOG_DELTA_REWRITE) {
      console.log(`
        The following subjects need to be explored for cascade removal: ${subjectsForPotentialCascadeRemoval.join('\n')}.
      `);
    }

    const updatedTypeCache = await buildTypeCache(
      service_config, service_export_config, { deletes: triplesToDelete });
    const extraTriplesToDelete = await rewriteDeletedChangeset(service_config,
                                                               service_export_config,
                                                               subjectsForPotentialCascadeRemoval,
                                                               updatedTypeCache,
                                                               true
                                                              );
    triplesToDelete.push(...extraTriplesToDelete);
  }

  return triplesToDelete;
}

/**
 * Enrich the delete changeset with resources that become irrelevant for the export
 * based on the triples in the original delete changeset.

 * Ie. 'deeper' resources that now don't have a complete path to the export concept scheme anymore.
 * Eg. deletion of a mandatee may cause the deletion of the person resource as well.
 *
 * The function is recursively applied to delete additional related resources
 * for the resources that have just been added to the delete changeset.
 * Eg. deletion of the person in the previous example may cause the deletion
 * of the person's birthdate as well.
 */
async function enrichDeletedChangeset(service_config, service_export_config, changeSet, typeCache, processedResources) {
  const impactedResources = getImpactedResources(service_config, service_export_config, changeSet, typeCache);

  const triplesToDelete = [];
  for (let {uri, config} of impactedResources) {
    if (!processedResources.includes(uri)) {
      processedResources.push(uri); // make sure to handle each resource only once
      const isOutOfScope = !(await isInScopeOfConfiguration(service_config,service_export_config, uri, config));
      if (isOutOfScope) {
        if (LOG_DELTA_REWRITE)
          console.log(`Enriching delete changeset with export of resource <${uri}>.`);
        const resourceExport = await exportResource(service_config, uri, config);
        triplesToDelete.push(...resourceExport);

        if (LOG_DELTA_REWRITE)
          console.log(`Recursively checking for enrichments based on the newly deleted triples for resource <${uri}>.`);
        const recursiveTypeCache = await buildTypeCache(service_config, service_export_config, {inserts: [], deletes: resourceExport});
        const recursiveTriples = await enrichDeletedChangeset(service_config, service_export_config, resourceExport, recursiveTypeCache, processedResources);
        triplesToDelete.push(...recursiveTriples);
      }
    } else {
      if (LOG_DELTA_REWRITE)
        console.log(`Resource <${uri}> already removed from export for given config. Ignoring now.`);
    }
  }

  return triplesToDelete;
}

/**
 * Get all possibly impacted resources by a changeset of triples. Impacted resources are subject/objects
 * of the triples in the changeSet for which a relation (predicate) has been inserted/deleted, that may
 * cause the resource to be included/excluded from the export.
 *
 * This function is the most complex part of the delta rewriting. The reasoning, which is explained in
 * inline documentation, can be best understood by creating a diagram of all configured types to export
 * and their path to the export concept scheme.
 *
 * Note: this function returns any possibly impacted resources. Whether the resource is
 * (ir)relevant for the export (ie. it has/doesn't have a path to the export concept scheme)
 * is validated in the enrichInserted/DeletedChangeset functions.
 */
function getImpactedResources(service_config, service_export_config, changeSet, typeCache) {
  const resources = [];

  const relations = changeSet.filter(t => t.object.type == 'uri');
  if (LOG_DELTA_REWRITE)
    console.log(
        `Found ${relations.length} triples in the changeset that are relations to other resources. They may possibly impact the export.`);

  for (let triple of relations) {
    // Add all subjects of triples with a predicate that equals the last (deepest) path segment
    // to the export CS in the export configuration of the subject's type
    // E.g. on insertion of the triple <mandatee-x> org:holds <mandate-a>
    // the full mandatee-x resource needs to be inserted (similar for delete)
    const subject = triple.subject.value;
    const exportConfigurations = typeCache.filter(e => e.uri == subject).map(e => e.config);
    if (LOG_DELTA_REWRITE)
      console.log(`Subject ${subject} has ${exportConfigurations.length} export configurations.`);

    const subjectExports = exportConfigurations.filter(
        config => config.pathToConceptScheme[0] == triple.predicate.value);
    for (let config of subjectExports) {
      if (LOG_DELTA_REWRITE)
        console.log(
            `Relation triple with subject <${subject}> with type <${config.type}> and predicate <${triple.predicate.value}> may have an impact on the export.`);
      resources.push({uri: subject, config: config});
    }

    // Add all objects of triples with a predicate that is the inverse of the last path segment
    // to the export CS in the export configuration of the object's type
    // E.g. on insertion of the triple <mandate-a> besluit:isBestuurlijkeAliasVan <person-x>
    // the full person-x resource needs to be inserted (similar for delete)
    const object = triple.object.value;
    const objectTypes = uniq(typeCache.filter(e => e.uri == object).map(e => e.config.type));
    if (LOG_DELTA_REWRITE)
      console.log(`Object ${object} has ${objectTypes.length} types in the cache.`);

    if (objectTypes.length) {
      for (let config of exportConfigurations) {
        const deeperConfigurations = getChildConfigurations(service_export_config, config)
            .filter(config => objectTypes.includes(config.type) && config.pathToConceptScheme[0] ==
                `^${triple.predicate.value}`);
        for (let deeperConfig of deeperConfigurations) {
          if (LOG_DELTA_REWRITE)
            console.log(
                `Relation triple with object <${object}> with type <${deeperConfig.type}> and predicate <${triple.predicate.value}> may have an impact on the export.`);
          resources.push({uri: object, config: deeperConfig});
        }
      }
    }
  }

  if (LOG_DELTA_REWRITE)
    console.log(
        `Found ${resources.length} triple that may cause the insertion/deletion of additional resources in the export.`);

  return resources;
}

/**
 * Construct the triples to be exported (inserted/deleted) for a given subject URI
 * based on the export configuration and the triples in the triplestore
 */
async function exportResource(
  service_config,
  uri,
  config,
  fromPublicationGraph
) {

  const rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
  const delta = [];

  // Helper function at append results from DB to delta-compliant format
  const appendQueryResult = (result, delta, configuredPred) => {
    for (let b of result.results.bindings) {
        const relatedUri = b['o'].value;
        if (isInverse(configuredPred)) {
          delta.push({
            subject: {type: 'uri', value: relatedUri},
            predicate: {type: 'uri', value: normalizePredicate(configuredPred)},
            object: {type: 'uri', value: uri}
          });
        } else {
          delta.push({
            subject: {type: 'uri', value: uri},
            predicate: {type: 'uri', value: normalizePredicate(configuredPred)},
            object: b['o']
          });
        }
      }
  };

  if(fromPublicationGraph) {
    const allProperties = [ ...config.properties, rdfType ];
    for (let prop of allProperties) {
      const q = `
        SELECT DISTINCT ?object WHERE {
          GRAPH ${sparqlEscapeUri(service_config.publicationGraph)} {
            ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(config.type)};
              ${sparqlEscapePredicate(prop)} ?o.
          }
        }
      `;
      const result = await query(
        q, {},
        { sparqlEndpoint: PUBLICATION_MU_AUTH_ENDPOINT, mayRetry: true }
      );
      appendQueryResult(result, delta, prop);
    }
  }
  else {
    delta.push({
      subject: {type: 'uri', value: uri},
      predicate: {type: 'uri', value: rdfType},
      object: {type: 'uri', value: config.type}
    });

    let additionalFilter = '';

    if (config.additionalFilter) {
      additionalFilter = config.additionalFilter;
    }

    for (let prop of config.properties) {
      // We skip this information because we already encoded it in the previous step
      // And we don't want to export too much (i.e. multi-types)
      // Note: this is a extra safety barrier
      if (prop == rdfType && config.strictTypeExport) {
        continue;
      }

      //Note the PublicationGraph is blacklisted -> it should not ONLY reside in the publicationGraph
      const q = `
        SELECT DISTINCT ?o WHERE {
          GRAPH ?graph {
            ${sparqlEscapeUri(uri)} ${sparqlEscapePredicate(prop)} ?o.
          }

          ${additionalFilter ? additionalFilter : ''}

          ${generateSourceGraphFilter(service_config, config)}
        }`;
      const result = await query(q);
      appendQueryResult(result, delta, prop);
    }

  }

  return delta;
}

/**
 * Returns child configurations a given config.
 * Ie. all configurations with a path that is 1 segment deeper than the given configuration.
 */
function getChildConfigurations(service_export_config, config) {
  return service_export_config.export.filter(child => {
    return child.pathToConceptScheme.length == config.pathToConceptScheme.length + 1
        && isSamePath(child.pathToConceptScheme.slice(1), config.pathToConceptScheme);
  });
}

/**
 * Returns whether the given subject is controlled by the configuration:
 * Note:
 *   this is based on the graphsFilter and conceptScheme, not the type.
 *   This function expects this check on type has been performed previously..
 *   TODO: refactor this tacit assumption. But there is a complexity with deletion, which need furhter thinking.
 * Note 2:
 *    by default the PublicationGraph is blacklisted -> it should not ONLY reside in the publicationGraph
 */
async function isInScopeOfConfiguration(service_config,
                                        service_export_config,
                                        subject, config,
                                        graphFilterBuilder = () => generateSourceGraphFilter(service_config, config)) {

  let additionalFilter = '';

  if (config.additionalFilter) {
    additionalFilter = config.additionalFilter;
  }

  let pathToConceptSchemeString = '';

  if (config.pathToConceptScheme.length) {
    const predicatePath = config.pathToConceptScheme.map(p => sparqlEscapePredicate(p)).join('/');
    pathToConceptSchemeString = `?subject ${predicatePath} ${sparqlEscapeUri(service_export_config.conceptScheme)}.`;
  }

  // Important note: renaming variables in the next query, will very likely break
  // additionalFilter functionality. So better leave it as is.
  // This is abstraction leakage, it might be in need in further thinking, but
  // it avoids for now the need for a complicated intermediate abstraction.

  const q = `
    SELECT ?predicate WHERE {
      BIND(${sparqlEscapeUri(subject)} as ?subject)

      ${pathToConceptSchemeString}

      GRAPH ?graph {
        ?subject ?predicate ?object .
      }

      ${additionalFilter}

      ${graphFilterBuilder()}

    } LIMIT 1
  `;

  const result = await query(q);
  return result.results.bindings.length;
}

function generateSourceGraphFilter(service_config, config) {

  const { additionalFilter,
          pathToConceptScheme,
          graphsFilter,
          hasRegexGraphsFilter,
          type,
          strictTypeExport,
          healingOptions
        } = config;

  // Either we want the triple to reside in a specific (set) of graphs,
  // or not (exclusively) in the publication graph.
  let graphsFilterStr = `FILTER(?graph NOT IN (${sparqlEscapeUri(service_config.publicationGraph)}))`;

  if(!hasRegexGraphsFilter) {
    if(graphsFilter.length > 0) {
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

  return graphsFilterStr;
}

function isConfiguredForExport(triple, config) {
  const predicate = triple.predicate.value;
  const rdfType = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type';
  // To ensure what gets exported exaclty matches the configuration,
  // we need to make sure the type matches. Else we might potentially export too much
  if (predicate == rdfType && config.strictTypeExport) {
    return triple.object.value == config.type;
  } else if (predicate == rdfType || config.properties.includes(predicate)) {
    return true;
  } else {
    return false;
  }
}

function diffDeltas(source, target){
  const targetHash = target.reduce((acc, curr) => {
    acc[serializeTriple(source)] = curr;
    return acc;
  }, {});

  const sourceHash = source.reduce((acc, curr) => {
    acc[serializeTriple(target)] = curr;
    return acc;
  }, {});

  const onlyInSource = source.filter(t => !targetHash(serializeTriple(t)));
  const onlyInTarget = target.filter(t => !sourceHash(serializeTriple(t)));
  return { onlyInSource, onlyInTarget };
}
