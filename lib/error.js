import { uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { parseResult } from './utils';

export async function loadError( _config, subject ){
  const queryError = `
   ${_config.PREFIXES}
   SELECT DISTINCT ?graph ?error ?message WHERE {
     GRAPH ?graph {
       BIND(${ sparqlEscapeUri(subject) } as ?error)
       ?error oslc:message ?message.
       optional { ?error dct:created ?created }

      }
    }
  `;
  return parseResult(await query(queryError))[0];
}

export async function createError( _config, graph, message ){
  const id = uuid();
  const uri = _config.ERROR_URI_PREFIX + id;
  const created = new Date();

  const queryError = `
   ${_config.PREFIXES}
   INSERT DATA {
    GRAPH ${sparqlEscapeUri(graph)}{
      ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(_config.ERROR_TYPE)};
        mu:uuid ${id};
        dct:created ${sparqlEscapeDateTime(created)};
        oslc:message ${sparqlEscapeString(message)}.
    }
   }
  `;

  await update(queryError);
  return await loadError(uri);
}

export async function removeError( _config, error ){
  const queryError = `
    ${_config.PREFIXES}
    DELETE {
     GRAPH ?g {
       ?error a ?errorType;
         mu:uuid ?uuid;
         dct:created ?created;
         oscl:message ?message.
     }
   }
   WHERE {
    GRAPH ?g {
     BIND(${sparqlEscapeUri(error.error)} as ?error)
     ?error a ?errorType;
         mu:uuid ?uuid;
         oslc:message ?message.
      optional {?error dct:created ?created}
    }
   }`;
  await update(queryError);
}
