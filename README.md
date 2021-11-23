# delta-producer-publication-graph-maintainer

Producer service resposible for:
  - maintaining the publication graph which acts as a source of truth for the delta diff files generation
  - excuting the preriodic healing of the publication graph
  - excuting the initial sync of the publication graph

## Tutorials

The production of what is considered an interesting delta has two modes. A 'simple mode' and concept-base filtering.

### Add the service to a stack
Suppose you are interested in publishing all changes related to mandatarissen
Add the service to your `docker-compose.yml`:

```yaml
  delta-producer-publication-graph-maintainer-mandatarissen:
    image: lblod/delta-producer-publication-graph-maintainer
    volumes:
      - ./data/files:/share
      - ./config/producer/mandatarissen:/config
    environment:
      RELATIVE_FILE_PATH: "deltas/mandatarissen"
      PUBLICATION_GRAPH: 'http://redpencil.data.gift/id/deltas/producer/loket-mandatarissen-producer'
      HEALING_JOB_OPERATION: 'http://redpencil.data.gift/id/jobs/concept/JobOperation/deltaHealingMandatarissen'
      INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION: 'http://redpencil.data.gift/id/jobs/concept/JobOperation/deltas/initibalPublicationGraphSyncing/mandatarissen'

```
Next, configure the delta-notifier in `./config/delta/rules.js` to send delta messages for all changes:

```javascript
export default [
  {
    match: {
      // anything -> note you can tweak the performance here by filtering on graphs if you need to.
    },
    callback: {
      url: 'http://delta-producer-concept-scheme-based-publication-maintainer-mandatarissen/delta',
      method: 'POST'
    },
    options: {
      resourceFormat: 'v0.0.1',
      gracePeriod: 1000,
      ignoreFromSelf: true
    }
  }

  // Other delta listeners
]
```
### configuration file

## simple mode
By example:

```
{
  "export": [
    {
      "type": "http://data.vlaanderen.be/ns/besluit#Agendapunt",
      "graphsFilter": [ "http://mu.semte.ch/graphs/public" ],
      "properties": [
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://mu.semte.ch/vocabularies/core/uuid",
        "http://data.vlaanderen.be/ns/besluit#aangebrachtNa",
        "http://purl.org/dc/terms/description",
        "http://data.vlaanderen.be/ns/besluit#geplandOpenbaar",
        "http://data.vlaanderen.be/ns/besluit#heeftOntwerpbesluit",
        "http://purl.org/dc/terms/reference",
        "http://purl.org/dc/terms/title",
        "http://data.vlaanderen.be/ns/besluit#Agendapunt.type"
      ]
    }
  ]
}
```
The simple mode publishes evereything that matches the pattern above. Hence you might expose too much data or even not enough, please refer to `concept scheme filtering` for more complex scenarios.

### concept scheme filtering
Here, the logic to consider what is a viable logical block to produce for consumer is based on a concept scheme filtering.
All resources which need to be exported, need to have a path to this concept scheme. The path to the concept scheme acts as a filter on the resources to export.
See also `Why must the generated delta's of the application stack be rewritten by the producer?` for a more detailed explanation.
The logic of export may vary, we will need to make sure to keep reverse compatible extensions if needed in the future.

Add a migration with the export concept scheme to `./config/migrations/20200421151800-add-loket-mandatarissen-concept-scheme.sparql`

```
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

INSERT DATA {
  GRAPH <http://mu.semte.ch/graphs/public> {
    <http://lblod.data.gift/concept-schemes/0887b850-b810-40d4-be0f-cafd01d3259b> a skos:ConceptScheme ;
    mu:uuid "0887b850-b810-40d4-be0f-cafd01d3259b" ;
    skos:prefLabel "Loket mandatarissen export" .
    <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000000> skos:inScheme <http://lblod.data.gift/concept-schemes/0887b850-b810-40d4-be0f-cafd01d3259b> .
    <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000001> skos:inScheme <http://lblod.data.gift/concept-schemes/0887b850-b810-40d4-be0f-cafd01d3259b> .
    <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000002> skos:inScheme <http://lblod.data.gift/concept-schemes/0887b850-b810-40d4-be0f-cafd01d3259b> .
    <http://data.vlaanderen.be/id/concept/BestuurseenheidClassificatieCode/5ab0e9b8a3b2ca7c5e000003> skos:inScheme <http://lblod.data.gift/concept-schemes/0887b850-b810-40d4-be0f-cafd01d3259b> .
  }
}
```

A partial file might look like:

```json
{
  "conceptScheme": "http://lblod.data.gift/concept-schemes/0887b850-b810-40d4-be0f-cafd01d3259b",
  "export": [
    {
      "type": "http://mu.semte.ch/vocabularies/ext/MandatarisStatusCode",
      "graphsFilter": [ "http://mu.semte.ch/graphs/public" ],
      "pathToConceptScheme": [
        "^http://data.vlaanderen.be/ns/mandaat#status",
        "http://www.w3.org/ns/org#holds",
        "^http://www.w3.org/ns/org#hasPost",
        "http://data.vlaanderen.be/ns/mandaat#isTijdspecialisatieVan",
        "http://data.vlaanderen.be/ns/besluit#classificatie",
        "http://www.w3.org/2004/02/skos/core#inScheme"
      ],
      "properties": [
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "http://mu.semte.ch/vocabularies/core/uuid",
        "http://www.w3.org/2004/02/skos/core#prefLabel",
        "http://www.w3.org/2004/02/skos/core#scopeNote"
      ]
    }
    ]
}
```


Restart the updated services
```
docker-compose restart migrations delta-notifier dispatcher
```

Create the newly added service
```
docker-compose up -d
```

## Reference
### Configuration
#### Export configuration
The export configuration defining which triples must be produced per `rdf:type` is specified in `/config/export.js`.

The configuration object, which must be exported by default, contains the following keys:
* `conceptScheme`: URI of the export concept scheme. Each resource that is relevant for the export must have a path to this concept scheme.
* `export`: array of export configurations per resource type

An export configuration entry in the `export` arrays contains the following keys:
* `type`: URI of the rdf:type for which the configuration holds
* `pathToConceptScheme`: list of path segments (URIs) from the resource to the export concept scheme. If the path exists, the resource is in scope of the export. Otherwise the resource is out of scope.
* `properties`: list of predicates (URIs) to export for the resource type
* `graphsFilter`: optional list of graphs where the expected triples come from.
* `additionalFilter`: Optional custom filter tied to the resource you want to export. Assumes `?subject` as starting point to filter on.
* `strictTypeExport`: If a resource has multiple types and it should not be exported, set this to `true`

The `export` array may contain multiple entries for an `rdf:type`.
A resource may have multiple types and therefore map to multiple export configugrations, but the `pathToConceptScheme` must be identical in that case. The service only works under the assumption that a resource has only 1 way to the export concept scheme (but there may be multiple instances of that way).

#### Environment variables
The following enviroment variables can be optionally configured:
* `LOG_INCOMING_DELTA (default: "false")`: log the delta message as received from the delta-notifier to the console
* `LOG_DELTA_REWRITE (default: "false")`: verbose log output during the rewrite of the incoming delta to the resulting delta. Only useful for debugging purposes.
* `RELATIVE_FILE_PATH (default: "deltas")`: relative path of the delta files compared to the root folder of the file service that will host the files.
* `PUBLISHER_URI (default: "http://data.lblod.info/services/loket-producer")`: URI underneath which delta files will be saved.
* `JOBS_GRAPH (default: "http://mu.semte.ch/graphs/system/jobs")`: URI where the jobs and jobs information are stored
* `PUBLICATION_GRAPH (required)`: URI of the publication graph to maintain
* `INITIAL_PUBLICATION_GRAPH_SYNC_JOB_OPERATION (required)`: URI of the job operation for intial syncing to listen to.
* `HEALING_JOB_OPERATION (required)`: URI of the job operation for healing operation to listen to.
*  `REPORTING_FILES_GRAPH`: If a specific graph is needed for the reports (e.g. healing) add URI here.
*  `QUEUE_POLL_INTERVAL`: the queue is polled every minute by default.
*  `WAIT_FOR_INITIAL_SYNC`: wait for initial sync. Defaults to 'true', mainly meant to disable for debugging purposes
* `MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE (default: http://redpencil.data.gift/id/concept/muScope/deltas/publicationGraphMaintenance)`: can be configured to work with scopes in delta-notifier. This is fired when a updates are performed on the publication graph. Most of the services in your stack wouldn't be interested by this update, so best to add this in the deltanotifier configuration as scope to exclude. So we reduce load on the system, and potential confusion.
* `MU_CALL_SCOPE_ID_INITIAL_SYNC (default: http://redpencil.data.gift/id/concept/muScope/deltas/initialSync)` : can be configured to work with scopes in delta-notifier. This when the publication graph is initially synced. Most of the services in your stack (if not all) wouldn't be interested by this update, so best to add this in the deltanotifier configuration as scope to exclude.
* `HEALING_PATCH_GRAPH_BATCH_SIZE: Size of insert/delete batches. Defaults to 100 triples`
* `SKIP_MU_AUTH_INITIAL_SYNC: Initial sync is expensive. Mu-auth can be skipped here. But be aware about what this implies!`

### API
#### POST /delta
Endpoint that receives delta's from the [delta-notifier](https://github.com/mu-semtech/delta-notifier). The delta's are rewritten based on the configured export for mandatees. The resulting delta's are written to files that can be retrieved via the `GET /files` endpoint.

## Discussions
### What is a publication graph anyway?
The publication graph acts as an interemediate step in the delta (file) generation process. This graph represents the state of the data that should be consumed by a consumer. The serialization of the updates, i.e. how we inform or publish additions or removals to the graph, is left to other services.

### Why must the generated delta's of the application stack be rewritten by the producer?
Simply writing all incoming delta messages to a file if the subject's `rdf:type` is of interest and offering those files to a consumer service may look as a simple and adequate solution at first sight, but it isn't. This simple approach has two downsides:
1. **Too much data may be exposed to the consuming application.** If we would for example export all triples for subjects that have `foaf:Person` as a type, we may be exporting too many persons. Not only the persons that hold a mandate, but also persons that are for example related to a `foaf:Account`. A lot of irrelevant data will be synced to the consuming application that way.
2. **Data may be missing in the consuming application.** A change on one resource may have an impact that is a lot bigger than just the single resource for which a delta arrived. E.g. relating a mandatee to a mandate doesn't only affect the mandatee resource. It also makes the person related to the mandatee relevant for the consuming application. On its turn, including the person also makes the person's birthdate relevant for the consuming application, etc.

To determine which resources are relevant to export, the producer makes use of an 'export concept scheme'. All resource that have a path in the triple store from the resource URI to the concept scheme are considered relevant. When a resource changes, all 'deeper' resources are recursively checked whether they also become (ir)relevant for the export. 'Deeper' in this context doesn't mean a hierarchical relationship, but more hops from the resource to the concept scheme.

The path to follow from the resource URI to the concept scheme is specified in the export configuration per resource type.

This approach may lead to duplicate inserts of data (eg. relating a mandate to a bestuursorgaan will produce triples to insert every mandatee, person, person's birthdate, etc. related to the mandate, while some of this data may have been synced before), but it will never expose irrelevant data or sync too little data.

## Known limitations
* The service keeps an in-memory publication of delta's to write to a file. If the service is killed before the delta's have been written to a file, the delta's are lost. Hence, shortening the `DELTA_INTERVAL`, decreases the chance to loose data on restart.

## Roadmap
* Add support for a prefix map in the export configuration
* A couple of cases are not supported when producing mini deltas
  - multi typing of subjects need further elaboration
  - incoming deltas are not checked for graph, this might lead for weird behaviour (corrected by the healing process though).
  - Deletion of triples and related resources doesn't work, but more importantly needs further thinking.
    - In some cases we might produce conflicting information, e.g a person both being a mandataris and leidinggevenden.
