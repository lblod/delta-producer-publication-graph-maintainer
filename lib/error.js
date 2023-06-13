import { uuid, sparqlEscapeString, sparqlEscapeUri, sparqlEscapeDateTime } from 'mu';
import { querySudo as query, updateSudo as update } from '@lblod/mu-auth-sudo';
import { parseResult } from './utils';

export async function loadError(serviceConfig, subject ){
  const queryError = `
   ${serviceConfig.prefixes}
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

export async function createError(serviceConfig, graph, message ){
  const id = uuid();
  const uri = serviceConfig.errorUriPrefix + id;
  const created = new Date();

  const queryError = `
   ${serviceConfig.prefixes}
   INSERT DATA {
    GRAPH ${sparqlEscapeUri(graph)}{
      ${sparqlEscapeUri(uri)} a ${sparqlEscapeUri(serviceConfig.errorType)};
        mu:uuid ${id};
        dct:created ${sparqlEscapeDateTime(created)};
        oslc:message ${sparqlEscapeString(message)}.
    }
   }
  `;

  await update(queryError);
  return await loadError(uri);
}

export async function removeError(serviceConfig, error ){
  const queryError = `
    ${serviceConfig.prefixes}
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
