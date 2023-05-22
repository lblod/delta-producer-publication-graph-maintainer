FROM openlink/virtuoso-opensource-7:7.2.6-r8-g5a27710-alpine as virtuoso

FROM semtech/mu-javascript-template:1.5.0-beta.4

LABEL maintainer="Redpencil <info@redpencil.io>"
RUN apk add coreutils unixodbc
COPY --from=virtuoso /opt/virtuoso-opensource /usr/local/virtuoso-opensource
COPY etc/odbc.ini /etc
COPY etc/odbcinst.ini /etc
# see https://github.com/mu-semtech/mu-javascript-template for more info
