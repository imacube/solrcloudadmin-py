# SolrCloudAdmin

Python library for carrying out routine SolrCloud administrative tasks.

Requires python3.

# Configuration File

The scripts below look for a configuration file in the users home directory: `~/.solrcloudadmin.config`. Use `solrcloudadmin.config.sample` as an example of what it should contain. You can override the configuration file used by passing `--config` to any of the scripts.

The configuration file uses profiles. This way one configuration file can be used for multiple environments. The user only needs to specify the profile to use, this is done with `-p` or `--profile`.

# Scripts

These scripts carry out basic SolrCloud administrative tasks on the cluster. Each command has its own built in help with further details. Access it by passing either `-h` or `--help`.

- solrcloud-add-replica
  - Add a replica to a collection's shard
- solrcloud-cluster-details
  - SolrCloud node collection counts and unhealthy cores
- solrcloud-collection-count
  - SolrCloud node collection counts
- solrcloud-delete-down
  - Delete replicas that are down, won't delete if all replicas are down
- solrcloud-delete-replica
  - Delete a specific replica
- solrcloud-list-live-nodes
  - List live SolrCloud nodes
- solrcoud-migrate-collections
  - Migrate collections from one SolrCloud node to another node
- solrcloud-move-collection
  - Move a collection to a new node
- solrcloud-replica-count
  - Search for replicas with a count above or below a specified threshold
- solrcloud-shard-count
  - Search for collections with more or less then the specified shard count
- solrcloud-unhealthy-cores
  - Return a list of unhealthy cores
- solrcloud-view-collections
  - View a collections information
