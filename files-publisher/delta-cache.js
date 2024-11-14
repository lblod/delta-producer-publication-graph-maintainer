import _ from 'lodash';
import { updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs-extra';
import { query, sparqlEscapeDateTime, uuid } from 'mu';
import { storeError } from '../lib/utils';
import { MAX_DELTAS_PER_FILE, DELTA_FILES_PAGINATION_MAX_PER_PAGE, MAX_TRIPLES_PER_OPERATION_IN_DELTA_FILE, PRETTY_PRINT_DIFF_JSON } from "../env-config";

const SHARE_FOLDER = '/share';

export default class DeltaCache {

  constructor() {
    this.cache = [];
  }

  /**
   * Push new entries to the delta cache
   *
   * @public
  */
  push() {
    this.cache.push(...arguments);
  }

  /**
   * Write current state of the delta cache to a file
   *
   * @public
  */
  async generateDeltaFile(service_config) {
    if (this.cache.length) {
      const cachedArray = [...this.cache];
      this.cache = [];

      const chunkedArray = chunkCache(service_config, cachedArray);
      for (const [index, entry] of chunkedArray.entries()) {
        try {
          const folderDate = new Date();
          const subFolder = folderDate.toISOString().split('T')[0];
          const outputDirectory = `${SHARE_FOLDER}/${service_config.relativeFilePath}/${subFolder}`;
          fs.mkdirSync(outputDirectory, { recursive: true });

          const filename = `delta-${new Date().toISOString()}-${index}.json`;
          const filepath = `${outputDirectory}/${filename}`;

          if (PRETTY_PRINT_DIFF_JSON) {
            await fs.writeFile(filepath, JSON.stringify(entry, null, 2));
          }
          else {
            await fs.writeFile(filepath, JSON.stringify(entry));
          }

          console.log(`Delta cache has been written to file. Cache contained ${entry.length} items.`);

          await this.writeFileToStore(service_config, filename, filepath);
          console.log("File is persisted in store and can be consumed now.");

        } catch (e) {
          await storeError(service_config, e);
        }
      }
    } else {
      console.log("Empty cache. Nothing to save on disk");
    }
  }


  async countDeltaFiles(service_config, since) {
    console.log(`Retrieving count delta files since ${since}`);

    const result = await query(`
    ${service_config.prefixes}

    SELECT (COUNT (distinct *) as ?count) WHERE {
      SELECT distinct ?uuid ?filename ?created WHERE {
        ?s a nfo:FileDataObject ; 
            mu:uuid ?uuid ;
            nfo:fileName ?filename ;
            dct:publisher <${service_config.publisherUri}> ;
            dct:created ?created .
        ?file nie:dataSource ?s .

        FILTER (?created > "${since}"^^xsd:dateTime)
      } ORDER BY ?created
    }
  `);

    if (result.results.bindings?.length) {
      return parseInt(result.results.bindings[0].count?.value || 0);
    }
    return 0;
  }

  /**
   * Get all delta files produced since a given timestamp
   *
   * @param service_config the configuration to be used
   * @param since {string} ISO date time
   * @public
  */
  async getDeltaFiles(service_config, since, page) {
    const path = service_config.filesPath;
    console.log(`Retrieving delta files since ${since}`);
    const calculatePages = (totalCount, limit) => {
      return Math.ceil(totalCount / limit);
    };
    const count = await this.countDeltaFiles(service_config, since);
    console.log("nordine count", count);
    const totalPages = calculatePages(count, DELTA_FILES_PAGINATION_MAX_PER_PAGE);
    console.log("total pages:", totalPages);
    const getPage = async (page, limit) => {
      const offset = (page - 1) * limit;
      const result = await query(`
        ${service_config.prefixes}
        SELECT ?uuid ?filename ?created WHERE {
          SELECT distinct ?uuid ?filename ?created WHERE {
            ?s a nfo:FileDataObject ;
                mu:uuid ?uuid ;
                nfo:fileName ?filename ;
                dct:publisher <${service_config.publisherUri}> ;
                dct:created ?created .
            ?file nie:dataSource ?s .

            FILTER (?created > "${since}"^^xsd:dateTime)
          } ORDER BY ?created
        } LIMIT ${limit} OFFSET ${offset}`);

      console.log('theresult', result);
      return result.results.bindings.map(b => {
        return {
          type: 'files',
          id: b['uuid'].value,
          attributes: {
            name: b['filename'].value,
            created: b['created'].value
          }
        };
      });
    };
    if (page) {
      if (count === 0) {
        console.log("no result");
        return { count, page: [], links: { first: null, prev: null, next: null, self: null, last: null } };
      }
      console.log("getPage")
      const pageRes = await getPage(page, DELTA_FILES_PAGINATION_MAX_PER_PAGE);
      console.log("result:", pageRes);
      return {
        count,
        files: pageRes,
        links: {
          first: `${path}?since=${since}&page["number"]=1`,
          prev: page > 1 ? `${path}?since=${since}&page["number"]=${page - 1}` : null,
          next: page < totalPages ? `${path}?since=${since}&page["number"]=${page + 1}` : null,
          self: `${path}?since=${since}&page["number"]=${page}`,
          last: `${path}?since=${since}&page["number"]=${totalPages}`
        }
      }
    }
    const response = [];
    console.log("build response all")
    for (let currentPage = 1; currentPage <= totalPages; currentPage++) {
      const res = await getPage(currentPage, DELTA_FILES_PAGINATION_MAX_PER_PAGE);
      console.log("build response", res)
      response.push(...res);
    }
    return {
      count,
      files: response,
      links: undefined
    };

  }

  /**
   * @private
   */
  async writeFileToStore(service_config, filename, filepath) {
    const virtualFileUuid = uuid();
    const virtualFileUri = `http://data.lblod.info/files/${virtualFileUuid}`;
    const nowLiteral = sparqlEscapeDateTime(new Date());
    const physicalFileUuid = uuid();
    const physicalFileUri = filepath.replace(SHARE_FOLDER, 'share://');

    await update(`
    ${service_config.prefixes}

    INSERT DATA {
      GRAPH <${service_config.filesGraph}> {
        <${virtualFileUri}> a nfo:FileDataObject ;
          mu:uuid "${virtualFileUuid}" ;
          nfo:fileName "${filename}" ;
          dct:format "application/json" ;
          dbpedia:fileExtension "json" ;
          dct:created ${nowLiteral} ;
          dct:modified ${nowLiteral} ;
          dct:publisher <${service_config.publisherUri}> .
        <${physicalFileUri}> a nfo:FileDataObject ;
          mu:uuid "${physicalFileUuid}" ;
          nie:dataSource <${virtualFileUri}> ;
          nfo:fileName "${filename}" ;
          dct:format "application/json" ;
          dbpedia:fileExtension "json" ;
          dct:created ${nowLiteral} ;
          dct:modified ${nowLiteral} .
      }
    }
  `, { 'mu-call-scope-id': service_config.muCallScopeIdPublicationGraphMaintenance });
  }
}

/**
 * Chunks the cached array, to not exploded memory when writing to json
 * @param service_config the configuration to be used
 * @param cache: [ { inserts: [], deletes: [] }, { inserts: [], deletes: [] } ]
 * @return [ [ { inserts: [], deletes: [] } ], [ { inserts: [], deletes: [] } ] ]
 */
function chunkCache(service_config, cache) {
  const allChunks = [];
  for (const entry of cache) {

    //results in [ [<uri_1>, ..., <uri_n>], [<uri_1>, ..., <uri_n>] ]
    const insertChunks = _.chunk(entry.inserts, MAX_TRIPLES_PER_OPERATION_IN_DELTA_FILE);
    const deleteChunks = _.chunk(entry.deletes, MAX_TRIPLES_PER_OPERATION_IN_DELTA_FILE);

    if (deleteChunks.length > 1 || insertChunks.length > 1) {
      for (const deleteChunk of deleteChunks) {
        const chunk = { inserts: [], deletes: deleteChunk };
        allChunks.push(chunk);
      }

      for (const insertChunk of insertChunks) {
        const chunk = { inserts: insertChunk, deletes: [] };
        allChunks.push(chunk);
      }
    }
    else {
      allChunks.push(entry);
    }
  }
  return _.chunk(allChunks, MAX_DELTAS_PER_FILE);
}
