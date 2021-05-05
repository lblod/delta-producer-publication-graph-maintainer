export const LOG_INCOMING_DELTA = process.env.LOG_INCOMING_DELTA || false;
export const DELTA_INTERVAL = process.env.DELTA_INTERVAL_MS || 1000;
export const LOG_DELTA_REWRITE = process.env.LOG_DELTA_REWRITE || false;
export const RELATIVE_FILE_PATH = process.env.RELATIVE_FILE_PATH || 'deltas';
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

export const ERROR_URI_PREFIX = 'http://redpencil.data.gift/id/cache-maintenance/error/';

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

//task operation of interest
export const HEALING_PATCH_CACHE_GRAPH_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/healing/deltas/patchCacheGraph';
export const INITIAL_CACHE_SYNC_TASK_OPERATION = 'http://redpencil.data.gift/id/jobs/concept/TaskOperation/deltas/initialCacheGraphSyncing';

//containers will keep track what has been healed etc for reports
export const REMOVAL_CONTAINER = 'http://redpencil.data.gift/id/concept/HealingProcess/RemovalContainer';
export const INSERTION_CONTAINER = 'http://redpencil.data.gift/id/concept/HealingProcess/InsertionContainer';

if(!process.env.CACHE_GRAPH)
  throw `Expected 'CACHE_GRAPH' should be provided.`;
export const CACHE_GRAPH = process.env.CACHE_GRAPH;

if(!process.env.INITIAL_CACHE_SYNC_JOB_OPERATION)
  throw `Expected 'INITIAL_CACHE_SYNC_JOB_OPERATION' should be provided.`;
export const INITIAL_CACHE_SYNC_JOB_OPERATION = process.env.INITIAL_CACHE_SYNC_JOB_OPERATION;

if(!process.env.HEALING_JOB_OPERATION)
  throw `Expected 'HEALING_JOB_OPERATION' should be provided.`;
export const HEALING_JOB_OPERATION = process.env.HEALING_JOB_OPERATION;
