
 A quick setup to start searching through the COVID-19 Open Research Dataset as provided at https://pages.semanticscholar.org/coronavirus-research .

 Run download_datasets.sh to get all the datasets.

 Then run container_setup.sh to get an Elasticsearch container up and running (listening to localhost:9200).19

 Then run the indexpapers.py script to populate the ES instance with all the papers (extracting fulltext, abstract, authors and author emails).

 Once you have the ES index all set up, you can either query it directly or build a client that does that for you.