# DFS
Distributed file system
It is very easy to launch DFS:  
1) Firstly, we need to configure and launch our minions. Minions use PORT: 8888, so
on your minion servers should be exposed port 8888. There are two ways how you can launch
minions: you can just run docker container from the ***cendien/dfs_minion*** image, or download
"Minion Server" folder and build image from dockerfile.
2) After the first step, we need to get IP addresses of the Minions. In master.py you should
change
``MINIONS = {"1": ("127.0.0.1", 8000),
             "2": ("127.0.0.1", 9000),}`` to ``MINIONS = {"1": ("IP1", 8888),
           "2": ("IP2", 8888),}``, where IP1 and IP2 are Ip adresses of the Minions you
           launched. You can also add as much Minions as you want. Also you can configure
           REPLICATION_FACTOR, by default it is 2, but it can be any number between 1 and N,
           where N is number of Minions.
3) After that you can launch Master. From ***cendien/dfs_master*** you can get our configuration
of the minions, if you want you own configuration you should launch Master from the "Master Server"
folder. Master uses PORT: 2131, so it should be exposed.
4) You do not need to launch Client, because it is just python script for the interaction
with the DFS. You just need to change ``host`` in client.py to the IP address of the Master.
5) Now you can use DFS. Just use ``python3 client.py command [parameters]`` with any
available command from the client.py.
