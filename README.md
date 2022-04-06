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
* `additionalFilter`: (experimental) Optional custom filter tied to the resource you want to export. Assumes `?subject` as starting point to filter on. (For limitations, cf. infra.)
* `strictTypeExport`: If a resource has multiple types and these extras should not be exported, set this to `true`

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
* `UPDATE_PUBLICATION_GRAPH_SLEEP (default: 1000): Sleep between batch updates in the update of the publication graph`

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
### additionalFilter
Additional filter should be used cautiously. It is very likely the configuration interface will change in the future.
#### simple filters
In live syncing mode, it will work for simple filters, not for deeply nested one. So, e.g.:
  - `?sForExport has:Status <http://someStatus/relevant/for/export>` will work, but
  - `?sForExport validatedBy <http://some/operator>. <http://some/operator> hasRole <http://can/validate/for/export>` won't work.

The healing compensates for this discrepancy, but be aware that as long as the healing hasn't run, your publication-graph (and thus your clients) might be in a weird state.
The reason complicated filters don't work: Suppose `?sForExport validatedBy <http://some/operator>` has been published, and in a second step, the `<http://some/operator>` gets a different role. This would imply the resource `?sForExport` should be retracted. In live syncing mode, there is no logic present to intercept such a trigger.
#### filters only in same graph
The filter will only work in the same graph as where the `?sToExport` resides.
Reason for this: current implementation makes it very difficult to exclude the publication graph when filtering (by default). Hence the limitation.

## Roadmap
* Add support for a prefix map in the export configuration
* A couple of cases are not supported when producing mini deltas
  - multi typing of subjects need further elaboration
  - incoming deltas are not checked for graph, this might lead for weird behaviour (corrected by the healing process though).
  - Deletion of triples and related resources doesn't work, but more importantly needs further thinking.
    - In some cases we might produce conflicting information, e.g a person both being a mandataris and leidinggevenden.
## Experimental features
### Support for publication graph residing in another triplestore
If publication graphs affect performance of the database too much, you can choose to store this information in another database.
By configuring the following environment variables:
```
  "PUBLICATION_VIRTUOSO_ENDPOINT": "http://virtuoso-2/sparql"
  "PUBLICATION_MU_AUTH_ENDPOINT": "http://database-2/sparql"
```
the service can maintain the publication graph residing in another database.
### SERVE_DELTA_FILES
By setting this to "true", this service creates and host delta-files. In essence it takes over the role of [delta-producer-json-diff-file-publisher](https://github.com/lblod/delta-producer-json-diff-file-publisher).
The motivation of merge this functionality back here:
  - Occam's razor applied applied on services for sane defaults: in many cases, the format of the published files will be the same as what is generated with `delta-producer-json-diff-file-publisher`.
    Hence, it was considered in practice cumbersome to instantiate another service for this.
  - If using another triplestore for the publication graph, this functionality might come in handy if you just want a triple store, not wrapped in a second instance of mu-auth. The serialization of the information to publish, is performed directly (if this feature is enabled) and not through deltas.
  - Increased robustness in the publication process.
    Since the serialization is not dependent on delta's, chances are reduced the published files and the publication graph get out of sync.

It might be this feature gets extended, i.e. more serialization formats, or completly removed. Practice will tell.

### Login + ACL
Published data may be wrapped up in an authorization layer, i.e. the delta files will only availble to agents with access.
It relies on mu-auth to follow the authorization scheme.
The following configuration needs to be added. Suppose the deltas to share are about 'persons-sensitive'.
#### docker-compose.yml
```
  # (...)
  KEY: "shared secret key between consumer and producer"
  ACCOUNT: "http://an/account/enabling/acces/to/delta-files-graph"
  FILES_GRAPH: "http://redpencil.data.gift/id/deltas/producer/persons-sensitive"
```
#### dispatcher
```
  post "/sync/persons-sensitive-deltas/login/*path" do
    Proxy.forward conn, path, "http://delta-producer-publication-graph-maintainer-instance/login/"
  end

  get "/sync/persons-sensitive-deltas/files/*path" do
    Proxy.forward conn, path, "http://delta-producer-publication-graph-maintainer-instance/files/"
  end
```
#### migrations
The ACL follows foaf model, as an example.
Alternative modeling of the authorization scheme is possible, the only key here is: the files-data should be stored in a specific graph (for now).
```
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
INSERT DATA {
  GRAPH <http://mu.semte.ch/graphs/diff-producer/login> { #Note this graph is configurable
     <http://an/account/enabling/acces/to/delta-files-graph>  a foaf:OnlineAccount.

     <http://the/agent/who/may/access/persons-sensitive-deltas> a foaf:Agent;
         foaf:account <http://an/account/enabling/acces/to/delta-files-graph>.

     <http://the/group/of/agents/allowed/persons-sensitive-deltas> a foaf:Group;
       foaf:member <http://the/agent/who/may/access/persons-sensitive-deltas>;
       foaf:name "persons-sensitive-deltas".
  }
}
```
#### authorization
Alternative modeling of the authorization scheme is possible, the only key here is: the files-data should be stored in a specific graph (for now).
Furthermore, better to be as restrictive as possible, i.e. not using the mu-auth feature of writing data to multiple graphs.
```
# (...)
defmodule Acl.UserGroups.Config do

  defp can_access_deltas_persons_sensitive_data() do
    %AccessByQuery{
      vars: [ ],
      query: "
        PREFIX foaf: <http://xmlns.com/foaf/0.1/>
        PREFIX muAccount: <http://mu.semte.ch/vocabularies/account/>
        SELECT DISTINCT ?onlineAccount WHERE {
          <SESSION_ID> muAccount:account ?onlineAccount.

          ?onlineAccount  a foaf:OnlineAccount.

          ?agent a foaf:Agent;
            foaf:account ?onlineAccount.

          <http://the/group/of/agents/allowed/persons-sensitive-deltas> foaf:member ?agent;
            foaf:name \"persons-sensitive-deltas\".
        }"
      }
  end

  def user_groups do
    [

      %GroupSpec{
        name: "o-persons-sensitive-deltas-rwf",
        useage: [:read, :write, :read_for_write],
        access: can_access_deltas_persons_sensitive_data(),
        graphs: [ %GraphSpec{
                    graph: "http://redpencil.data.gift/id/deltas/producer/persons-sensitive",
                    constraint: %ResourceConstraint{
                      resource_types: [
                        "http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject",
                        "http://www.w3.org/ns/dcat#Dataset", # is needed in dump file
                        "http://www.w3.org/ns/dcat#Distribution",
                      ] } } ] },
    ]
  end

end
```
