FROM semtech/mu-javascript-template:1.5.0-beta.4

LABEL maintainer="Redpencil <info@redpencil.io>"
RUN apk add coreutils unixodbc
# see https://github.com/mu-semtech/mu-javascript-template for more info
