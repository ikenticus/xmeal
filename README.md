# XMeaL


## Definition

XMeaL (ex-em-EEL):
a generic XML ingestor that handles retrieving, sorting, parsing and ingesting of XML data into an ODBC database.  The name is an amalgam of the word XML and meal.


## Actions

* pull         retrieve "last" files(s) from all specified feeds
* sort         separates all the pulled files into classifier folders
* parse        extracts all the data from the sorted files based on the rules
* push         insert all the parsed data into the db, initiating any load sprocs
* post         instead of directly pushing to db, utilize API to POST data
* cache        push all parsed data to specified cache mechanism via template
* purge        deletes all the files in the skipped and failed folders

Multiple actions can be utilize by listing them comma-delimited



