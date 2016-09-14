# 0.16.1 (2016-09-14)

Changed order of `solrcloud-cluster-details` final output,
'Unhealthy collections' will be printed first then collection counts.

# 0.16.0 (2016-08-26)

Added simple script to get count of documents in a collection.

# 0.15.0 (2016-08-25)

Added summary of collection statuses to cluster details.

# 0.14.4 (2016-08-22)

The old library is gone and no longer supported. The new library is: `solrcloudadmin/collections_api.py`.

* Changed license from GNU GPL-v2 to Apache License, Version 2.0
* Rewrote library and scripts to use python3
* Made pip installable again
* All scripts start with: `solrcloud-`
* Added common configuration file with profiles to be used by all scripts

# 0.5.0 (2015-08-16)

* Made pip installable
* Moved scripts to scripts/ and dropped .py extensions
