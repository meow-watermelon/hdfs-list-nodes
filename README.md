# hdfs-list-nodes

**Intro**

hdfs-list-nodes is a small tool to display Hadoop datanode(s) states.

**Usage**

```
usage: hdfs-list-nodes.py [-h] -u URL -s STATE

HDFS DataNode State List

optional arguments:
  -h, --help            show this help message and exit
  -u URL, --url URL     full namenode url. <http(s)://namenode-hostname:http-
                        port>
  -s STATE, --state STATE
                        datanode state. <live|live-and-decom|decom-
                        ing|dead|dead-and-decom>

Display Format:
DataNode<tab>State<tab>LastContactTimestamp<tab>LastContactSecond(s)
```

*-u*: namenode HTTP url with port.

*-s*: specify datanode state to be displayed. available states flags are **live**, **live-and-decom**, **decom-ing**, **dead**, **dead-and-decom**. 

**Notes**

*live*: In service

*live-and-decom*: Decommissioned and still live

*decom-ing*: Under decommission process

*dead*: Lost heartbeat and marked as dead node

*dead-and-decom*: Decommissioned and marked as dead node
