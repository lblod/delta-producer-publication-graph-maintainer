import { sparqlEscapeUri, uuid, sparqlEscapeString, sparqlEscapeDateTime, sparqlEscapeInt } from 'mu';
import fs from 'fs-extra';
import { updateSudo as update } from '@lblod/mu-auth-sudo';
/**
 * Write the given TTL content to a file and relates it to the given remote file and submitted document
 *
 * @param string ttl Turtle to write to the file
 * @param string submittedDocument URI of the submittedDocument to relate the new TTL file to
 * @param string remoteFile URI of the remote file to relate the new TTL file to
*/
export async function writeTtlFile(graph, content, logicalFileName) {
  const phyId = uuid();
  const phyFilename = `${phyId}.ttl`;
  const path = `/share/${phyFilename}`;
  const physicalFile = path.replace('/share/', 'share://');
  const loId = uuid();
  const logicalFile = `http://data.lblod.info/id/files/${loId}`;
  const now = new Date();

  try {
    await fs.writeFile(path, content, 'utf-8');
  } catch (e) {
    console.log(`Failed to write TTL to file <${physicalFile}>.`);
    throw e;
  }

  try {
    const stats = await fs.stat(path);
    const fileSize = stats.size;

    await update(`
      PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
      PREFIX nie: <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX dct: <http://purl.org/dc/terms/>
      PREFIX dbpedia: <http://dbpedia.org/ontology/>
      INSERT DATA {
        GRAPH ${sparqlEscapeUri(graph)} {
          ${sparqlEscapeUri(physicalFile)} a nfo:FileDataObject;
                                  nie:dataSource ${sparqlEscapeUri(logicalFile)} ;
                                  mu:uuid ${sparqlEscapeString(phyId)};
                                  nfo:fileName ${sparqlEscapeString(phyFilename)} ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created ${sparqlEscapeDateTime(now)};
                                  dct:modified ${sparqlEscapeDateTime(now)};
                                  dct:format "text/turtle";
                                  nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                  dbpedia:fileExtension "ttl".
          ${sparqlEscapeUri(logicalFile)} a nfo:FileDataObject;
                                  mu:uuid ${sparqlEscapeString(loId)};
                                  nfo:fileName ${sparqlEscapeString(logicalFileName)} ;
                                  dct:creator <http://lblod.data.gift/services/harvesting-import-service>;
                                  dct:created ${sparqlEscapeDateTime(now)};
                                  dct:modified ${sparqlEscapeDateTime(now)};
                                  dct:format "text/turtle";
                                  nfo:fileSize ${sparqlEscapeInt(fileSize)};
                                  dbpedia:fileExtension "ttl" .
        }
      }
`);

  } catch (e) {
    console.log(`Failed to write TTL resource <${logicalFile}> to triplestore.`);
    throw e;
  }

  return logicalFile;
}
