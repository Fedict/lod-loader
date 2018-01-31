# GraphDB loader

> Imports file into GraphDB

---

## Why this tool

- Abstract/hide SPARQL API

--

## How it works

- One "watch" directory per repository
- HTTP POST of zip with RDF or RDF + query
- Contents processed in 1 DB transaction
- Failed ZIP moved to "failed" directory

---

## Limitations

- Currently only HTTP basic authentication
- Little feedback during importing
  - States "in process" or "done", not % 

---

## Technology

- [DropWizard](http://www.dropwizard.io) REST server
- Java [WatchService](https://docs.oracle.com/javase/tutorial/essential/io/notification.html) API
- [RDFJ4](http://rdf4j.org/) Java API

---

## Thank you

Questions ? 

@fa[twitter] @BartHanssens
