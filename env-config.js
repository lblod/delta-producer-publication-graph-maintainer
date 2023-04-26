export class Config {
    constructor() {
        this.LOG_INCOMING_DELTA = process.env.LOG_INCOMING_DELTA || false;
        this.LOG_DELTA_REWRITE = process.env.LOG_DELTA_REWRITE || false;
        this.PUBLISHER_URI = process.env.PUBLISHER_URI || 'http://data.lblod.info/services/loket-producer';
        this.PREFIXES = `
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
        this.ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/publication-maintenance/error/';
        this.JOBS_GRAPH = process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';
        this.JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
        this.TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';
        this.STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
        this.STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
        this.STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
        this.STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';
        this.STATUS_CANCELED = 'http://redpencil.data.gift/id/concept/JobStatus/canceled';
        this.ERROR_TYPE = 'http://open-services.net/ns/core#Error';
        this.DELTA_ERROR_TYPE = 'http://redpencil.data.gift/vocabularies/deltas/Error';
        this.ERROR_CREATOR_URI = process.env.ERROR_CREATOR_URI || 'http://lblod.data.gift/services/delta-producer-publication-graph-maintainer';

        //task operation of interest
        this.HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/healing/patchPublicationGraph';
        this.INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/initialPublicationGraphSyncing';

        //containers will keep track what has been healed etc for reports
        this.REMOVAL_CONTAINER = 'http://redpencil.data.gift/id/concept/HealingProcess/RemovalContainer';
        this.INSERTION_CONTAINER = 'http://redpencil.data.gift/id/concept/HealingProcess/InsertionContainer';

        this.REPORTING_FILES_GRAPH = process.env.REPORTING_FILES_GRAPH;
        this.QUEUE_POLL_INTERVAL = process.env.QUEUE_POLL_INTERVAL || 60000;

        this.HEALING_PATCH_GRAPH_BATCH_SIZE = parseInt(process.env.HEALING_PATCH_GRAPH_BATCH_SIZE || 100);
        this.UPDATE_PUBLICATION_GRAPH_SLEEP = parseInt(process.env.UPDATE_PUBLICATION_GRAPH_SLEEP || 1000);
        this.SKIP_MU_AUTH_DELTA_FOLDING = process.env.SKIP_MU_AUTH_DELTA_FOLDING == 'true' ? true : false;

        this.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE = process.env.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE
            || 'http://redpencil.data.gift/id/concept/muScope/deltas/publicationGraphMaintenance';

        this.MU_CALL_SCOPE_ID_INITIAL_SYNC = process.env.MU_CALL_SCOPE_ID_INITIAL_SYNC
            || 'http://redpencil.data.gift/id/concept/muScope/deltas/initialSync';

        //mainly for debugging purposes
        this.WAIT_FOR_INITIAL_SYNC = process.env.WAIT_FOR_INITIAL_SYNC == 'false' ? false : true;

        if (!process.env.PUBLICATION_GRAPH)
            throw `Expected 'PUBLICATION_GRAPH' should be provided.`;
        this.PUBLICATION_GRAPH = process.env.PUBLICATION_GRAPH;

        if (!process.env.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)
            throw `Expected 'INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION' should be provided.`;
        this.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION = process.env.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION;

        if (!process.env.HEALING_JOB_OPERATION)
            throw `Expected 'HEALING_JOB_OPERATION' should be provided.`;
        this.HEALING_JOB_OPERATION = process.env.HEALING_JOB_OPERATION;

        /*
         * START EXPERIMENTAL FEATURES
         */
        //SKIP MU_AUTH
        this.USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS = process.env.USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS == 'true' ? true : false;
        this.SKIP_MU_AUTH_INITIAL_SYNC = process.env.SKIP_MU_AUTH_INITIAL_SYNC == 'true' ? true : false;
        this.VIRTUOSO_ENDPOINT = process.VIRTUOSO_ENDPOINT || 'http://virtuoso:8890/sparql';
        this.MU_AUTH_ENDPOINT = process.MU_AUTH_ENDPOINT || 'http://database:8890/sparql';

        //DIFFERENT ENDPOINT FOR PUBLICATION GRAPH
        this.PUBLICATION_VIRTUOSO_ENDPOINT = process.env.PUBLICATION_VIRTUOSO_ENDPOINT || this.VIRTUOSO_ENDPOINT;
        this.PUBLICATION_MU_AUTH_ENDPOINT = process.env.PUBLICATION_MU_AUTH_ENDPOINT || this.MU_AUTH_ENDPOINT;

        //FILES PUBLISHER
        this.SERVE_DELTA_FILES = process.env.SERVE_DELTA_FILES || false;
        this.LOG_OUTGOING_DELTA = process.env.LOG_OUTGOING_DELTA || false;
        this.DELTA_INTERVAL = process.env.DELTA_INTERVAL_MS || 1000;
        this.PRETTY_PRINT_DIFF_JSON = process.env.PRETTY_PRINT_DIFF_JSON == 'true';
        this.ERROR_GRAPH = process.env.ERROR_GRAPH || 'http://mu.semte.ch/graphs/system/errors';
        this.RELATIVE_FILE_PATH = process.env.RELATIVE_FILE_PATH || 'deltas';
        this.FILES_GRAPH = process.env.FILES_GRAPH || 'http://mu.semte.ch/graphs/public';

        this.CACHE_CHUNK_STATEMENT = parseInt(process.env.CACHE_CHUNK_STATEMENT || 100);
        this.CACHE_CHUNK_ARRAY = parseInt(process.env.CACHE_CHUNK_ARRAY || 10);

        //LOGIN
        this.KEY = process.env.KEY || '';
        this.ACCOUNT = process.env.ACCOUNT || 'http://services.lblod.info/diff-consumer/account';
        this.ACCOUNT_GRAPH = process.env.ACCOUNT_GRAPH || 'http://mu.semte.ch/graphs/diff-producer/login';

        /*
         * END EXPERIMENTAL FEATURES
         */

         /*
          * PATHS
          */
        this.DELTA_PATH = '/delta';
        this.FILES_PATH = '/files';
        this.LOGIN_PATH = '/login';
    }
}
