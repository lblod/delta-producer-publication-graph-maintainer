export const LOG_INCOMING_DELTA = process.env.LOG_INCOMING_DELTA || false;
export const LOG_DELTA_REWRITE = process.env.LOG_DELTA_REWRITE || false;
export const PUBLISHER_URI = process.env.PUBLISHER_URI || 'http://data.lblod.info/services/loket-producer';

export const PREFIXES = `
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

export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/publication-maintenance/error/';

export const JOBS_GRAPH =  process.env.JOBS_GRAPH || 'http://mu.semte.ch/graphs/system/jobs';
export const JOB_TYPE = 'http://vocab.deri.ie/cogs#Job';
export const TASK_TYPE = 'http://redpencil.data.gift/vocabularies/tasks/Task';

export const STATUS_BUSY = 'http://redpencil.data.gift/id/concept/JobStatus/busy';
export const STATUS_SCHEDULED = 'http://redpencil.data.gift/id/concept/JobStatus/scheduled';
export const STATUS_SUCCESS = 'http://redpencil.data.gift/id/concept/JobStatus/success';
export const STATUS_FAILED = 'http://redpencil.data.gift/id/concept/JobStatus/failed';
export const STATUS_CANCELED = 'http://redpencil.data.gift/id/concept/JobStatus/canceled';

export const ERROR_TYPE= 'http://open-services.net/ns/core#Error';
export const DELTA_ERROR_TYPE = 'http://redpencil.data.gift/vocabularies/deltas/Error';
export const ERROR_CREATOR_URI = process.env.ERROR_CREATOR_URI || 'http://lblod.data.gift/services/delta-producer-publication-graph-maintainer';

//task operation of interest
export const HEALING_PATCH_PUBLICATION_GRAPH_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/healing/patchPublicationGraph';
export const INITIAL_PUBLICATION_GRAPH_SYNC_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/initialPublicationGraphSyncing';

//containers will keep track what has been healed etc for reports
export const REMOVAL_CONTAINER = 'http://redpencil.data.gift/id/concept/HealingProcess/RemovalContainer';
export const INSERTION_CONTAINER = 'http://redpencil.data.gift/id/concept/HealingProcess/InsertionContainer';

export const REPORTING_FILES_GRAPH = process.env.REPORTING_FILES_GRAPH;
export const QUEUE_POLL_INTERVAL = process.env.QUEUE_POLL_INTERVAL || 60000;

export const HEALING_PATCH_GRAPH_BATCH_SIZE = parseInt(process.env.HEALING_PATCH_GRAPH_BATCH_SIZE || 100);
export const UPDATE_PUBLICATION_GRAPH_SLEEP = parseInt(process.env.UPDATE_PUBLICATION_GRAPH_SLEEP || 1000);
export const SKIP_MU_AUTH_DELTA_FOLDING = process.env.SKIP_MU_AUTH_DELTA_FOLDING == 'true' ? true : false ;

export const MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE = process.env.MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE
  || 'http://redpencil.data.gift/id/concept/muScope/deltas/publicationGraphMaintenance';

export const MU_CALL_SCOPE_ID_INITIAL_SYNC = process.env.MU_CALL_SCOPE_ID_INITIAL_SYNC
  || 'http://redpencil.data.gift/id/concept/muScope/deltas/initialSync';

//mainly for debugging purposes
export const WAIT_FOR_INITIAL_SYNC = process.env.WAIT_FOR_INITIAL_SYNC == 'false' ? false : true ;

if(!process.env.PUBLICATION_GRAPH)
  throw `Expected 'PUBLICATION_GRAPH' should be provided.`;
export const PUBLICATION_GRAPH = process.env.PUBLICATION_GRAPH;

if(!process.env.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION)
  throw `Expected 'INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION' should be provided.`;
export const INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION = process.env.INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION;

if(!process.env.HEALING_JOB_OPERATION)
  throw `Expected 'HEALING_JOB_OPERATION' should be provided.`;
export const HEALING_JOB_OPERATION = process.env.HEALING_JOB_OPERATION;

/*
 * START EXPERIMENTAL FEATURES
 */

//SKIP MU_AUTH
export const USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS = process.env.USE_VIRTUOSO_FOR_EXPENSIVE_SELECTS == 'true' ? true : false ;
export const SKIP_MU_AUTH_INITIAL_SYNC = process.env.SKIP_MU_AUTH_INITIAL_SYNC == 'true' ? true : false ;
export const VIRTUOSO_ENDPOINT = process.VIRTUOSO_ENDPOINT  || 'http://virtuoso:8890/sparql';
export const MU_AUTH_ENDPOINT = process.MU_AUTH_ENDPOINT || 'http://database:8890/sparql';

//DIFFERENT ENDPOINT FOR PUBLICATION GRAPH
export const PUBLICATION_VIRTUOSO_ENDPOINT = process.env.PUBLICATION_VIRTUOSO_ENDPOINT || VIRTUOSO_ENDPOINT;
export const PUBLICATION_MU_AUTH_ENDPOINT = process.env.PUBLICATION_MU_AUTH_ENDPOINT || MU_AUTH_ENDPOINT;

//FILES PUBLISHER
export const SERVE_DELTA_FILES = process.env.SERVE_DELTA_FILES || false;
export const LOG_OUTGOING_DELTA = process.env.LOG_OUTGOING_DELTA || false;
export const DELTA_INTERVAL = process.env.DELTA_INTERVAL_MS || 1000;
export const PRETTY_PRINT_DIFF_JSON = process.env.PRETTY_PRINT_DIFF_JSON == 'true';
export const ERROR_GRAPH =  process.env.ERROR_GRAPH || 'http://mu.semte.ch/graphs/system/errors';
export const RELATIVE_FILE_PATH = process.env.RELATIVE_FILE_PATH || 'deltas';
export const FILES_GRAPH = process.env.FILES_GRAPH || 'http://mu.semte.ch/graphs/public';

export const CACHE_CHUNK_STATEMENT = parseInt(process.env.CACHE_CHUNK_STATEMENT || 100);
export const CACHE_CHUNK_ARRAY = parseInt(process.env.CACHE_CHUNK_ARRAY || 10);

//LOGIN
export const KEY = process.env.KEY || '';
export const ACCOUNT = process.env.ACCOUNT || 'http://services.lblod.info/diff-consumer/account';
export const ACCOUNT_GRAPH = process.env.ACCOUNT_GRAPH || 'http://mu.semte.ch/graphs/diff-producer/login';

/*
 * END EXPERIMENTAL FEATURES
 */
