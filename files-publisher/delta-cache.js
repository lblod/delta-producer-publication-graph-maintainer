import _ from 'lodash';
import { updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs-extra';
import { query, sparqlEscapeDateTime, uuid } from 'mu';
import { storeError } from '../lib/utils';
import {
    FILES_GRAPH,
    MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE, PREFIXES,
    PRETTY_PRINT_DIFF_JSON, PUBLISHER_URI,
    RELATIVE_FILE_PATH
} from '../env-config';

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
  async generateDeltaFile() {
    if (this.cache.length) {
      const cachedArray = this.cache;
      this.cache = [];

      const chunkedArray = chunkCache(cachedArray);
      for(const entry of chunkedArray) {
        try {
          const folderDate = new Date();
          const subFolder = folderDate.toISOString().split('T')[0];
          const outputDirectory = `${SHARE_FOLDER}/${RELATIVE_FILE_PATH}/${subFolder}`;
          fs.mkdirSync(outputDirectory, { recursive: true });

          const filename = `delta-${new Date().toISOString()}.json`;
          const filepath = `${outputDirectory}/${filename}`;

          if(PRETTY_PRINT_DIFF_JSON){
            await fs.writeFile(filepath, JSON.stringify( entry, null, 2 ));
          }
          else {
            await fs.writeFile(filepath, JSON.stringify( entry ));
          }

          console.log(`Delta cache has been written to file. Cache contained ${entry.length} items.`);

          await this.writeFileToStore(filename, filepath);
          console.log("File is persisted in store and can be consumed now.");

        } catch (e) {
          await storeError(e);
        }
      }
    } else {
      console.log("Empty cache. Nothing to save on disk");
    }
  }

  /**
   * Get all delta files produced since a given timestamp
   *
   * @param since {string} ISO date time
   * @public
  */
  async getDeltaFiles(since) {
    console.log(`Retrieving delta files since ${since}`);

    const result = await query(`
    ${PREFIXES}

    SELECT ?uuid ?filename ?created WHERE {
      ?s a nfo:FileDataObject ;
          mu:uuid ?uuid ;
          nfo:fileName ?filename ;
          dct:publisher <${PUBLISHER_URI}> ;
          dct:created ?created .
      ?file nie:dataSource ?s .

      FILTER (?created > "${since}"^^xsd:dateTime)
    } ORDER BY ?created
  `);

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
  }

  /**
   * @private
   */
  async writeFileToStore(filename, filepath) {
    const virtualFileUuid = uuid();
    const virtualFileUri = `http://data.lblod.info/files/${virtualFileUuid}`;
    const nowLiteral = sparqlEscapeDateTime(new Date());
    const physicalFileUuid = uuid();
    const physicalFileUri = filepath.replace(SHARE_FOLDER, 'share://');

    await update(`
    ${PREFIXES}

    INSERT DATA {
      GRAPH <${FILES_GRAPH}> {
        <${virtualFileUri}> a nfo:FileDataObject ;
          mu:uuid "${virtualFileUuid}" ;
          nfo:fileName "${filename}" ;
          dct:format "application/json" ;
          dbpedia:fileExtension "json" ;
          dct:created ${nowLiteral} ;
          dct:modified ${nowLiteral} ;
          dct:publisher <${PUBLISHER_URI}> .
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
  `, { 'mu-call-scope-id': MU_CALL_SCOPE_ID_PUBLICATION_GRAPH_MAINTENANCE });
  }
}

/**
 * Chunks the cached array, to not exploded memory when writing to json
 * @param cache: [ { inserts: [], deletes: [] }, { inserts: [], deletes: [] } ]
 * @return [ [ { inserts: [], deletes: [] } ], [ { inserts: [], deletes: [] } ] ]
 */
function chunkCache( cache ) {
  const allChunks = [];
  for(const entry of cache){

    //results in [ [<uri_1>, ..., <uri_n>], [<uri_1>, ..., <uri_n>] ]
    const insertChunks = _.chunk(entry.inserts, 100);
    const deleteChunks = _.chunk(entry.deletes, 100);

    if(deleteChunks.length > 1 || insertChunks.length > 1 ){
      for(const deleteChunk of deleteChunks){
        const chunk = { inserts: [], deletes: deleteChunk };
        allChunks.push(chunk);
      }

      for(const insertChunk of insertChunks){
        const chunk = { inserts: insertChunk, deletes: [] };
        allChunks.push(chunk);
      }
    }
    else {
      allChunks.push(entry);
    }
  }
  return _.chunk(allChunks, 10);
}
