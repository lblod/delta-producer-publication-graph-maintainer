import bodyParser from 'body-parser';
import { app, errorHandler, sparqlEscapeUri, sparqlEscapeString, sparqlEscapeDateTime, uuid } from 'mu';
import { querySudo, updateSudo } from '@lblod/mu-auth-sudo';
import { Config, CONFIG_SERVICES_JSON_PATH, ERROR_URI_PREFIX, DELTA_ERROR_TYPE, ERROR_TYPE, PREFIXES } from './env-config';
import DeltaPublisher from './files-publisher/delta-publisher';
import { executeHealingTask } from './jobs/healing/main';
import { updatePublicationGraph } from './jobs/publishing/main';
import { doesDeltaContainNewTaskToProcess, hasInitialSyncRun, isBlockingJobActive } from './jobs/utils';
import { ProcessingQueue } from './lib/processing-queue';
import { loadConfiguration, storeError } from './lib/utils';
import { setupDeltaProcessorForconfig, setupDeltaFileEndpoint, setupDelaLoginEndpoint } from './producer-setup-utils';

app.use( bodyParser.json({
  type: function(req) { return /^application\/json/.test( req.get('content-type') ); },
  limit: '500mb'
}));

let services = require(CONFIG_SERVICES_JSON_PATH);

// This variable will bundle the deltaStreamHandler per export type.
// This will allow us to dispatch incoming deltas to the correct handler.
// { 'streamName': handler, configuredTypes }
const configuredTypesPerHandler = {};

console.log("Services config is: ", services);
for (const name in services){
  let service = services[name];
  const service_config = new Config(service, name);
  const service_export_config = loadConfiguration(service_config.exportConfigPath);

  const producerQueue = new ProcessingQueue(service_config);
  const deltaPublisher = new DeltaPublisher(service_config);

  const deltaProcessor = setupDeltaProcessorForconfig(service_config,
                                                      service_export_config,
                                                      producerQueue,
                                                      deltaPublisher);

  const configuredTypes = service_export_config.export.map(c => c.type);
  configuredTypes.push(service_config.taskType); //it still needs to listen to Tasks for healing etc

  configuredTypesPerHandler[name] = { handler: deltaProcessor, configuredTypes };

  if(service_config.deltaPath) {
    app.post(service_config.deltaPath, deltaProcessor);
  }

  if (service_config.serveDeltaFiles) {
    //This endpoint only makes sense if serveDeltaFiles is set to true;
    app.get(service_config.filesPath, setupDeltaFileEndpoint(deltaPublisher));
  }
  // This endpoint can be used by the consumer to get a session
  // This is useful if the data in the files is confidential
  // Note that you will need to configure mu-auth so it can make sense out of it
  // TODO: probably this functionality will move somewhere else
  app.post(service_config.loginPath, setupDelaLoginEndpoint(service_config));
}

app.post("/delta", async function(req, res) {
  try {
    const delta = req.body;
    const allTypes = await extractTypesFromDelta(delta);
    await dispatchRequest(req, res, allTypes);
  }
  catch (error) {
    console.error(error);
    storeDispatchingError(services, error);
    res.status(500).send();
  }
});


/*****************************************************
 * HELPERS
 *****************************************************/
async function extractTypesFromDelta(delta) {
  let allTypes = [];
  let allUris = [];

  // TODO: this is very similar logic as in -buildTypeCache-.

  // First get types from delta
  for (let changeSet of delta) {
    const triples = [...changeSet.inserts, ...changeSet.deletes];

    const typesFromChangeset = triples
          .filter(t => t.predicate.value == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
          .map(t => t.object.value);
    allTypes.push(...typesFromChangeset);

    // gather all involved URI's so to deduce values from store
    allUris.push(...triples.map(t => t.subject.value));
    allUris.push(...triples.filter(t => t.object.type == 'uri').map(t => t.object.value));
  }

  // Then add types from store
  allUris = [ ... new Set(allUris) ];
  console.log(`Found ${allUris.length} in delta, checking store for rdf:type`);

  // Some speed optimisation, if long querystring, mu-auth is slower and ask one by one
  if(allUris.length > 20 ) {
    for (let uri of allUris) {
      const result = await querySudo(`
        SELECT DISTINCT ?type WHERE {
            ${sparqlEscapeUri(uri)} a ?type.
        }
      `);
      const typesFromStore = result.results.bindings.map(b => b['type'].value);
      allTypes.push(...typesFromStore);
    }
  }
  else {
    const result = await querySudo(`
      SELECT DISTINCT ?type WHERE {
          VALUES ?subject {
             ${allUris
               .map(uri => sparqlEscapeUri(uri))
               .join('\n')}
          }
          ?subject a ?type.
      }
    `);
    const typesFromStore = result.results.bindings.map(b => b['type'].value);
    allTypes.push(...typesFromStore);
  }

  return allTypes;
}

async function dispatchRequest(req, res, allTypes) {
  // TODO: note; probs the dispatching can even be made more efficient, but would required bigger refactor.
  for(const streamName of Object.keys(configuredTypesPerHandler)) {
    const configuredTypes = configuredTypesPerHandler[streamName].configuredTypes;
    const handler = configuredTypesPerHandler[streamName].handler;
    if(configuredTypes.some(confType => allTypes.some(allType => confType == allType))) {
      console.log(`
        Delta producer stream: ${streamName} WILL process a delta containing the following types:
        ${allTypes.join('\n')}
      `);
      handler(req, res);
    }
  };
}

let timerId;
async function storeDispatchingError(servicesConfig, errorMsg) {

  const creationTS = new Date().toISOString();

  const storeError = async function () {
    try {
      const id = uuid();
      const uri = ERROR_URI_PREFIX + id;

      const fullErrorMsg = `
        A general error occured during the dispatching of a delta to delta producer stream.".
        Error Message: ${errorMsg}.
      `;

      //TODO: a lot of complication arises from the way we can configure different streams
      let graphs = [];
      const creators = [];
      for (const name in services){
        let service = services[name];
        const service_config = new Config(service, name, false);
        graphs.push(`${sparqlEscapeUri(service_config.jobsGraph)}`);
        creators.push(`dct:creator ${sparqlEscapeUri(service_config.errorCreatorUri)};`);
      }

      graphs = [...new Set(graphs)];
      const partialInsertBlocks  = graphs.map(g => `
            GRAPH ${g} {
             ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)}.
            }
      `);

      const queryError = `
          ${PREFIXES}

          INSERT DATA {
            GRAPH ${graphs[0]} {
              ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(ERROR_TYPE)}, ${sparqlEscapeUri(DELTA_ERROR_TYPE)};
                mu:uuid ${sparqlEscapeString(id)};
                dct:subject "Delta Producer Publication Graph Maintainer" ;
                oslc:message ${sparqlEscapeString(fullErrorMsg)};
                ${[...new Set(creators)].join('\n')}
                dct:created ${sparqlEscapeDateTime(creationTS)}.
            }
            ${partialInsertBlocks.join('\n')}
          }

        `;
      await updateSudo(queryError);
    }
    catch (error) {
      console.error('Error storing error...');
      console.error(error);
    }
  };

  // Debounce 10 s
  clearTimeout(timerId);
  timerId = setTimeout(storeError, 10000);
}

app.use(errorHandler);
