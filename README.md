# lod-loader
LOD tool for loading data into RDF4J server (e.g. Ontotext GraphDB).

## Loading / updating data

### Bulk upload

Triples in an N-Triples file (extension .nt) will be loaded in the triple store.

### Queries with 1 parameter

CSV files containing 1 RDF URI per line can be used as input for similarly named
query files (`qr`). This URI will be bound to the `id` variable in the prepared
SPARQL query.

If no query file is submitted, the upload tool will look for a similarly named
query file on the server.

E.g. `activities.csv` will be used as input for `activities.qr`. 
If the file is not sent, the server will try to load 
`/processRoot/repo/query/activities.qr`

### Submitting files

The files - even if there is only one - must be submitted as a ZIP, 
e.g. a `file.zip` for repository `repo`, 
uploaded to `example.host` with basic HTTP authentication.

```
curl https://example.host/_upload/repo --basic -u userme:passme 
	    -X POST -H "Content-Type: application/zip" --data-binary @file.zip
```

All the files in the ZIP will be part of the same transaction.

It is recommended to pause a few seconds between submitting ZIPs.

## Files

Files will be stored within the `processRoot` directory, using one subdirectory 
per repository (or "namespace" in Ontotext GraphDB speak).

```
/processRoot/repo/process (temp directory when processing)
/processRoot/repo/query   (optional dir with default queries)
/processRoot/repo/upload  (upload directory)
```


## Configuration

Example config file

```
auth:
   username: userme
   password: passme
   
storage:
   sparqlPoint: "http://localhost:7200"
   username: loaduser
   password: passuser
   processRoot: ./load

server:
  requestLog:
    appenders:
      - type: file
        archive: false
        currentLogFilename: ./request.log

logging:
  level: INFO
  appenders:
    - type: file
      archive: false
      currentLogFilename: ./application.log
    - type: console
      target: stdout

```

`processRoot`is the root (top-level) directory where the query files are stored and
files will be uploaded.
