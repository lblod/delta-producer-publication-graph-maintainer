import { app, errorHandler, sparqlEscapeUri, uuid } from 'mu';
import { updateSudo } from '@lblod/mu-auth-sudo';
import {
  LOG_INCOMING_DELTA
} from './env-config';
import { executeHealingTask } from './jobs/healing/main';
import { updatePublicationGraph } from './jobs/publishing/main';
import { doesDeltaContainNewTaskToProcess, hasInitialSyncRun, isBlockingJobActive } from './jobs/utils';
import { storeError } from './lib/utils';

export function setupDeltaProcessorForconfig(service_config,
  service_export_config,
  producerQueue, deltaPublisher) {

  return async function (req, res) {
    try {
      const body = req.body;

      if (LOG_INCOMING_DELTA)
        console.log(`Receiving delta ${JSON.stringify(body)}`);

      if (await doesDeltaContainNewTaskToProcess(service_config, body)) {
        //From here on, the database is source of truth and the incoming delta was just a signal to start
        console.log(`Healing process (or initialSync) will start.`);
        console.log(`There were still ${producerQueue.queue.length} jobs in the queue`);
        console.log(`And the queue executing state is on ${producerQueue.executing}.`);
        producerQueue.queue = []; //Flush all remaining jobs, we don't want moving parts cf. next comment
        producerQueue.addJob(async () => {
          return await executeHealingTask(service_config, service_export_config);
        });
      } else if (await isBlockingJobActive(service_config)) {
        // During the healing (and probably initial sync too) we want as few as much moving parts,
        // If a delta comes in while the healing process is busy, this might yield inconsistent/difficult to troubleshoot results.
        // Suppose:
        //  - healing produces statement S1 at t1: "REMOVE <foo> <bar> <baz>."
        //  - random service produces statement S2 at t2: "ADD <foo> <bar> <baz>."
        //  - Note: Processing of statements has two phases, updating the publication graph (PH1) and in later step creating the delta file for syncing (PH2)
        //  - Suppose S1 and S2 are correctly processed in order for PH1, but S2 is processed in PH2 before S1. (Because, e.g. healing takes more time)
        //  This would result in publication graph and its delta-files counterpart are out of sync. Which affects the clients information too.
        //  Eventual consistency with another run of the healing process wouldn't fix this, because a healing process relies on the publication graph
        //  as it's primarily source of information. And in the above case, publication graph will be considered fine by the healing process.
        //
        // By blocking a potentially interesting delta, this situation should be mitigated in many cases.
        // Furthermore, the current delta will be missed by the publication graph, but should be recovered by the next run of the
        // healing process.
        //
        // Note: This blocking is not bulletproof. Other cases where it fails: Healing starts before a statement has been processed in PH2.
        // The process is highly async and at the time of writing I miss all the possible race conditions.
        // It needs to be seen how often the above issue occurs. Potential paths to mitigate this (but really needs harder thinking)
        //  - A post-healing process, a periodic replay of the delta-files to compare it with the publication graph and update the publication graph to reflect
        //    the state of the delta-files (or other kind of serializations)
        //  - A block on the delta-file service when healing job is busy and work with intermediate graphs of ADDITIONS and REMOVALS to be handled
        //    by the delta-file service itself
        //  - Some kind of multi lock system, where all the services involved should tell they are ready to be healed.
        console.info('Blocking jobs are active, skipping incoming deltas');
      } else if (service_config.waitForInitialSync && !await hasInitialSyncRun(service_config)) {
        // To produce and publish consistent deltas an initial sync needs to have run.
        // The initial sync job produces a dump file which provides a cartesian starting point for the delta diff files to make sense on.
        // It doesn't produce delta diff files, because performance.
        // All consumers are assumed to first ingest the delta dump, before consuming the diff files.
        // If delta-diff files are published before the initial sync and consumers already have ingested these, we run into troubles.
        // Note: waitForInitialSync is mainly meant for debugging purposes, defaults to true
        console.info('Initial sync did not run yet, skipping incoming deltas');
      } else {
        //normal operation mode: maintaining the publication graph
        //Put in a queue, because we want to make sure to have them ordered.
        producerQueue.addJob(async () => await runPublicationFlow(service_config, service_export_config, deltaPublisher, body));
      }
      res.status(202).send();
    } catch (error) {
      console.error(error);
      await storeError(service_config, error);
      res.status(500).send();
    }
  };
}

export function setupDeltaFileEndpoint(deltaPublisher) {
  return async function (req, res) {
    const page = req.query.page?.number;
    const { count, files, links } = await deltaPublisher.getDeltaFiles(req.query.since, page ? parseInt(page) : undefined);
    res.json({ meta: { count }, data: files, links });
  };
}

export function setupDeltaLoginEndpoint(service_config) {
  return async function (req, res) {
    try {

      // 0. To avoid false sense of security, login only makes sense if accepted key is configured
      if (!service_config.key) {
        throw "No key configured in service.";
      }

      // 1. get environment info
      const sessionUri = req.get('mu-session-id');

      // 2. validate credentials
      if (req.get("key") !== service_config.key) {
        throw "Key does not match";
      }

      // 3. add new login to session
      await updateSudo(`
      PREFIX muAccount: <http://mu.semte.ch/vocabularies/account/>
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(service_config.account_graph)} {
          ${sparqlEscapeUri(sessionUri)} muAccount:account ${sparqlEscapeUri(service_config.account)}.
        }
      }`);

      // 4. request login recalculation
      return res
        .header('mu-auth-allowed-groups', 'CLEAR')
        .status(201)
        .send({
          links: {
            self: '/sessions/current'
          },
          data: {
            type: 'sessions',

            id: uuid()
          }
        });
    } catch (e) {
      console.error(e);
      return res.status(500).send({ message: "Something went wrong" });
    }
  };
}

async function runPublicationFlow(service_config, service_export_config, deltaPublisher, deltas) {
  try {
    const insertedDeltaData = await updatePublicationGraph(service_config, service_export_config, deltas);
    if (service_config.serveDeltaFiles) {
      await deltaPublisher.publishDeltaFiles(insertedDeltaData);
    }
  } catch (error) {
    console.error(error);
    await storeError(service_config, error);
  }
}
