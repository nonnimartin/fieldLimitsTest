# fieldLimitsTest

Tool to create large number of Solr documents with a chosen number of fields.

Requires enlighten library. This can be installed with `pip install enlighten`

Use config.json to configure Solr endpoint, and to set the number of fields, documents, and the size of json payload for each request.

-c flag will override the config.json "commit" setting and set the requests to commit=true, otherwise the script defaults to the configuration.i

-i flag will enable "incremental" creation of new fields, starting from where the last run of the script left off
