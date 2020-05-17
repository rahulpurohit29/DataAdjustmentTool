# Data Adjustment Tool
#### An adjustment tool to make corrections to data that has been processed earlier. The data is stored in a distributed storage system like HDFS.

#### Basic functionality:
1. The correction details for an entity can be uploaded as csv using a front end tool. This csv will be
uploaded to Hadoop.
2. The csv will have only those fields filled in which needs to be corrected, along with the record
identifier ( pkey of the entity ), all other fields remains blank.
3. In the csv, we can put a string “__NULL__” for any field that needs to be updated as NULL.
4. The adjustments csv when processed by adjustment tool, it will create a new version of entity
record.
