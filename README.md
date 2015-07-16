DCAP SYNC
========

Sync local directory with into dcache (a very early steps).


Features
--------
syncs changes files. Keeps old versions.
smimple dcap client written with python

The configuration in dcapsync.conf:
-----------------
```
[scheduler]
fs = /home/tigran/toSync

[store]
path = /exports/data/Backup
```

where *scheduler.fs* points to local directory to sync and *store.path* points to remote path

