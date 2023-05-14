export const LOG_INCOMING_DELTA = process.env.LOG_INCOMING_DELTA || false;
export const LOG_DELTA_REWRITE = process.env.LOG_DELTA_REWRITE || false;
export const VIRTUOSO_ENDPOINT = process.VIRTUOSO_ENDPOINT || 'http://virtuoso:8890/sparql';
export const MU_AUTH_ENDPOINT = process.MU_AUTH_ENDPOINT || 'http://database:8890/sparql';

//DIFFERENT ENDPOINT FOR PUBLICATION GRAPH
export const PUBLICATION_VIRTUOSO_ENDPOINT = process.env.PUBLICATION_VIRTUOSO_ENDPOINT || VIRTUOSO_ENDPOINT;
export const PUBLICATION_MU_AUTH_ENDPOINT = process.env.PUBLICATION_MU_AUTH_ENDPOINT || MU_AUTH_ENDPOINT;
//FILES PUBLISHER
export const PRETTY_PRINT_DIFF_JSON = process.env.PRETTY_PRINT_DIFF_JSON === 'true';
//LOGIN
export const KEY = process.env.KEY || '';
export const ACCOUNT = process.env.ACCOUNT || 'http://services.lblod.info/diff-consumer/account';
export const ACCOUNT_GRAPH = process.env.ACCOUNT_GRAPH || 'http://mu.semte.ch/graphs/diff-producer/login';
export const CACHE_CHUNK_STATEMENT = parseInt(process.env.CACHE_CHUNK_STATEMENT || 100);
export const CACHE_CHUNK_ARRAY = parseInt(process.env.CACHE_CHUNK_ARRAY || 10);
export const CONFIG_SERVICES_JSON_PATH = process.env.CONFIG_SERVICES_JSON_PATH || '/config/services.json'

export class Config {
    constructor(configData) {
        this.exportConfigPath = configData.exportConfigPath;
        this.publisherUri = configData.publisherUri || 'http://data.lblod.info/services/loket-producer';
        this.prefixes = `
  PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
  PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
  PREFIX dct: <http://purl.org/dc/terms/>
  PREFIX prov: <http://www.w3.org/ns/prov#>
  PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
  PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
  PREFIX oslc: <http://open-services.net/ns/core#>
  PREFIX cogs: <http://vocab.deri.ie/cogs#>
  PREFIX adms: <http://www.w3.org/ns/adms#>
  PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
  PREFIX dbpedia: <http://dbpedia.org/resource/>
`;
        this.errorUriPrefix = 'http://redpencil.data.gift/id/publication-maintenance/error/';
        this.jobsGraph = configData.jobsGraph || 'http://mu.semte.ch/graphs/system/jobs';
        this.jobType = 'http://vocab.deri.ie/cogs#Job';
        this.taskType = 'http://redpencil.data.gift/vocabularies/tasks/Task';
        this.statusBusy = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
        this.statusScheduled = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
        this.statusSuccess = 'http://redpencil.data.gift/id/concept/JobStatus/success';
        this.statusFailed = 'http://redpencil.data.gift/id/concept/JobStatus/failed';
        this.statusCanceled = 'http://redpencil.data.gift/id/concept/JobStatus/canceled';
        this.errorType = 'http://open-services.net/ns/core#Error';
        this.deltaErrorType = 'http://redpencil.data.gift/vocabularies/deltas/Error';
        this.errorCreatorUri = configData.errorCreatorUri || 'http://lblod.data.gift/services/delta-producer-publication-graph-maintainer';

        //task operation of interest
        this.healingPatchPublicationGraphTaskOperation = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/healing/patchPublicationGraph';
        this.initialPublicationGraphSyncTaskOperation = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/initialPublicationGraphSyncing';

        //containers will keep track what has been healed etc for reports
        this.removalContainer = 'http://redpencil.data.gift/id/concept/HealingProcess/RemovalContainer';
        this.insertionContainer = 'http://redpencil.data.gift/id/concept/HealingProcess/InsertionContainer';

        this.reportingFilesGraph = configData.reportingFilesGraph;
        this.queuePollInterval = configData.queuePollInterval || 60000;

        this.healingPatchGraphBatchSize = parseInt(configData.healingPatchGraphBatchSize || 100);
        this.updatePublicationGraphSleep = parseInt(configData.updatePublicationGraphSleep || 1000);
        this.skipMuAuthDeltaFolding = configData.skipMuAuthDeltaFolding == 'true' ? true : false;

        this.muCallScopeIdPublicationGraphMaintenance = configData.muCallScopeIdPublicationGraphMaintenance
            || 'http://redpencil.data.gift/id/concept/muScope/deltas/publicationGraphMaintenance';

        this.muCallScopeIdInitialSync = configData.muCallScopeIdInitialSync
            || 'http://redpencil.data.gift/id/concept/muScope/deltas/initialSync';

        //mainly for debugging purposes
        this.waitForInitialSync = configData.waitForInitialSync == 'false' ? false : true;

        if (!configData.publicationGraph)
            throw `Expected 'publicationGraph' should be provided.`;
        this.publicationGraph = configData.publicationGraph;

        if (!configData.initialPublicationGraphSyncJobOperation)
            throw `Expected 'initialPublicationGraphSyncJobOperation' should be provided.`;
        this.initialPublicationGraphSyncJobOperation = configData.initialPublicationGraphSyncJobOperation;

        if (!configData.healingJobOperation)
            throw `Expected 'healingJobOperation' should be provided.`;
        this.healingJobOperation = configData.healingJobOperation;

        /*
         * START EXPERIMENTAL FEATURES
         */
        //SKIP MU_AUTH
        this.useVirtuosoForExpensiveSelects = configData.useVirtuosoForExpensiveSelects === 'true';
        this.skipMuAuthInitialSync = configData.skipMuAuthInitialSync === 'true';

        //FILES PUBLISHER
        this.serveDeltaFiles = configData.serveDeltaFiles || false;
        this.logOutgoingDelta = configData.logOutgoingDelta || false;
        this.deltaInterval = configData.deltaInterval || 1000;
        this.errorGraph = configData.errorGraph || 'http://mu.semte.ch/graphs/system/errors';
        this.relativeFilePath = configData.relativeFilePath || 'deltas';
        this.filesGraph = configData.filesGraph || 'http://mu.semte.ch/graphs/public';


        /*
         * END EXPERIMENTAL FEATURES
         */

        /*
         * PATHS
         */
        if (!configData.deltaPath)
            throw `Expected 'deltaPath' should be provided.`;
        this.deltaPath = configData.deltaPath;
        if (!configData.filesPath)
            throw `Expected 'filesPath' should be provided.`;
        this.filesPath = configData.filesPath;
        if (!configData.loginPath)
            throw `Expected 'loginPath' should be provided.`;
        this.loginPath = configData.loginPath;
    }
}
