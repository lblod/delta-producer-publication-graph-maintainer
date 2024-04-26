import { updateSudo, querySudo } from '@lblod/mu-auth-sudo';
import bodyParser from 'body-parser';
import { app, errorHandler, sparqlEscapeUri, uuid } from 'mu';
import {
  Config, CONFIG_SERVICES_JSON_PATH, LOG_INCOMING_DELTA
} from './env-config';
import DeltaPublisher from './files-publisher/delta-publisher';
import { executeHealingTask } from './jobs/healing/main';
import { updatePublicationGraph } from './jobs/publishing/main';
import { doesDeltaContainNewTaskToProcess, hasInitialSyncRun, isBlockingJobActive } from './jobs/utils';
import { ProcessingQueue } from './lib/processing-queue';
import {loadConfiguration, storeError} from './lib/utils';
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
  const delta = req.body;
  const allTypes = await extractTypesFromDelta(delta);
  await dispatchRequest(req, res, allTypes);
});

async function extractTypesFromDelta(delta) {
  let allTypes = [];
  let allUris = [];

  for (let changeSet of delta) {
    const triples = [...changeSet.inserts, ...changeSet.deletes];

    // First get types from delta
    const typesFromChangeset = triples
          .filter(t => t.predicate.value == 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type')
          .map(t => t.object.value);
    allTypes.push(...typesFromChangeset);

    // gather all involved URI's so to deduce values from store
    allUris.push(...triples.map(t => t.subject.value));
    allUris.push(...triples.filter(t => t.object.type == 'uri').map(t => t.object.value));
  }

  // from store
  allUris = [ ... new Set(allUris) ];
  console.log(`Found ${allUris.length} in delta, checking store for rdf:type`);

  for (let uri of allUris) {
    const result = await querySudo(`
      SELECT DISTINCT ?type WHERE {
          ${sparqlEscapeUri(uri)} a ?type.
      }
    `);
    const typesFromStore = result.results.bindings.map(b => b['type'].value);
    allTypes.push(...typesFromStore);
  }

  return allTypes;
}

async function dispatchRequest(req, res, allTypes) {
  for(const streamName of Object.keys(configuredTypesPerHandler)) {
    const configuredTypes = configuredTypesPerHandler[streamName].configuredTypes;
    const handler = configuredTypesPerHandler[streamName].handler;
    if(configuredTypes.some(confType => allTypes.some(allType => confType == allType))) {
      console.log(`
        Delta stream: ${streamName} --WILL-- process a delta containing the following types:
        ${allTypes.join('\n')}
      `);
      handler(req, res);
    }
    else {
      console.log(`
        Delta stream: ${streamName} will >>>WILL NOT<<< process a delta containing the following types:
         ${allTypes.join('\n')}
      `);
    }
  };
}

app.use(errorHandler);
