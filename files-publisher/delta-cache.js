import _ from 'lodash';
import { updateSudo as update } from '@lblod/mu-auth-sudo';
import fs from 'fs-extra';
import { query, sparqlEscapeDateTime, uuid } from 'mu';
import { storeError } from '../lib/utils';
import {CACHE_CHUNK_ARRAY, CACHE_CHUNK_STATEMENT, PRETTY_PRINT_DIFF_JSON} from "../env-config";

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
  async generateDeltaFile(serviceConfig) {
    if (this.cache.length) {
      const cachedArray = [ ...this.cache ];
      this.cache = [];

      const chunkedArray = chunkCache(serviceConfig, cachedArray);
      for(const [ index, entry ] of chunkedArray.entries()) {
        try {
          const folderDate = new Date();
          const subFolder = folderDate.toISOString().split('T')[0];
          const outputDirectory = `${SHARE_FOLDER}/${serviceConfig.relativeFilePath}/${subFolder}`;
          fs.mkdirSync(outputDirectory, { recursive: true });

          const filename = `delta-${new Date().toISOString()}-${index}.json`;
          const filepath = `${outputDirectory}/${filename}`;

          if(PRETTY_PRINT_DIFF_JSON){
            await fs.writeFile(filepath, JSON.stringify( entry, null, 2 ));
          }
          else {
            await fs.writeFile(filepath, JSON.stringify( entry ));
          }

          console.log(`Delta cache has been written to file. Cache contained ${entry.length} items.`);

          await this.writeFileToStore(serviceConfig, filename, filepath);
          console.log("File is persisted in store and can be consumed now.");

        } catch (e) {
          await storeError(serviceConfig, e);
        }
      }
    } else {
      console.log("Empty cache. Nothing to save on disk");
    }
  }

  /**
   * Get all delta files produced since a given timestamp
   *
   * @param serviceConfig the configuration to be used
   * @param since {string} ISO date time
   * @public
  */
  async getDeltaFiles(serviceConfig, since) {
    console.log(`Retrieving delta files since ${since}`);

    const result = await query(`
    ${serviceConfig.prefixes}

    SELECT ?uuid ?filename ?created WHERE {
      ?s a nfo:FileDataObject ;
          mu:uuid ?uuid ;
          nfo:fileName ?filename ;
          dct:publisher <${serviceConfig.publisherUri}> ;
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
  async writeFileToStore(serviceConfig, filename, filepath) {
    const virtualFileUuid = uuid();
    const virtualFileUri = `http://data.lblod.info/files/${virtualFileUuid}`;
    const nowLiteral = sparqlEscapeDateTime(new Date());
    const physicalFileUuid = uuid();
    const physicalFileUri = filepath.replace(SHARE_FOLDER, 'share://');

    await update(`
    ${serviceConfig.prefixes}

    INSERT DATA {
      GRAPH <${serviceConfig.filesGraph}> {
        <${virtualFileUri}> a nfo:FileDataObject ;
          mu:uuid "${virtualFileUuid}" ;
          nfo:fileName "${filename}" ;
          dct:format "application/json" ;
          dbpedia:fileExtension "json" ;
          dct:created ${nowLiteral} ;
          dct:modified ${nowLiteral} ;
          dct:publisher <${serviceConfig.publisherUri}> .
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
  `, { 'mu-call-scope-id': serviceConfig.muCallScopeIdPublicationGraphMaintenance });
  }
}

/**
 * Chunks the cached array, to not exploded memory when writing to json
 * @param serviceConfig the configuration to be used
 * @param cache: [ { inserts: [], deletes: [] }, { inserts: [], deletes: [] } ]
 * @return [ [ { inserts: [], deletes: [] } ], [ { inserts: [], deletes: [] } ] ]
 */
function chunkCache(serviceConfig, cache ) {
  const allChunks = [];
  for(const entry of cache){

    //results in [ [<uri_1>, ..., <uri_n>], [<uri_1>, ..., <uri_n>] ]
    const insertChunks = _.chunk(entry.inserts, CACHE_CHUNK_STATEMENT);
    const deleteChunks = _.chunk(entry.deletes, CACHE_CHUNK_STATEMENT);

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
  return _.chunk(allChunks, CACHE_CHUNK_ARRAY);
}
