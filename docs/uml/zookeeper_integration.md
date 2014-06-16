
ZooKeeper Integration for H2O
=============================

This document describes integration between H2O and ZooKeeper to
manage H2O clouds.  View this document as both Markdown and as a .png
rendered by PlantUML.


Use cases that are well thought out
-----------------------------------

* User starts a new H2O cloud


Use cases that need work
------------------------

* User shuts down an H2O cloud with the Web UI (Admin->Shutdown)
* H2O cloud node dies
* ZooKeeper unavailable at cloud start
* ZooKeeper unavailable at cloud shutdown
* Reaper of old H2O clouds


Contributers
------------

* Aswin Raveendran (eBay)
* Chris Severs (eBay)
* Tom Kraljevic (0xdata)


PlantUML diagram (render with jar file from http://plantuml.sourceforge.net)
============================================================================

@startuml

actor user

box "h2o-zookeeper.jar"
    participant water.zookeeper.h2odriver as driver
end box

box "h2o-zookeeper.jar (1)"
    participant "water.zookeeper.h2oworker (1)" as worker1
    participant "water.H2O (1)" as h2o1
end box

box "h2o-zookeeper.jar (2)"
    participant "water.zookeeper.h2oworker (2)" as worker2
    participant "water.H2O (2)" as h2o2
end box

box "zookeeper.jar"
    database zookeeper
end box



autonumber



== User starts a new H2O cloud ==

user -> driver : User starts driver process
activate driver
note left
  User specifies zkConnectionString: a.b.c.d:e
  User specifies zkRoot: /zk/path/h2o-uuid
  User specifies numNodes: 2

  java -jar h2o-zookeeper.jar water.zookeeper.h2odriver
      -zk a.b.c.d:e
      -zkroot "/zk/path/h2o-uuid"
      -nodes 2
      -start
end note

== Establish root ZNode for this cloud ==

driver -> zookeeper : Driver creates zkRoot
note left
  create("/zk/path/h2o-uuid", (PERSISTENT))
  ZNode data: { "numNodes" : 2 }

  User is responsible for providing
  a unique cloud name (h2o-uuid).
end note

driver <- zookeeper : OK

driver -> zookeeper : Driver creates zkRoot/nodes
note left
  create("/zk/path/h2o-uuid/nodes", (PERSISTENT))
end note

driver <- zookeeper : OK

user <- driver : OK
deactivate driver

== Start workers ==

user -> worker1 : Start H2O node 1
activate worker1
note left
  java -jar h2o-zookeeper.jar water.zookeeper.h2oworker
      -zk a.b.c.d:e
      -zkroot "/zk/path/h2o-uuid"
      -nodes 2
      [plus other H2O options]
end note
worker1 -> h2o1 : Start H2O node 1
activate h2o1
worker1 <- h2o1 : H2O node 1 port chosen
worker1 -> zookeeper : Worker creates ZNode
note left
  create("/zk/path/h2o-uuid/nodes/", (PERSISTENT, SEQUENCE))
  ZNode data: { "ip" : "n1a.n1b.n1c.n1d", "port" : n1e, "pid" : pid1 }
end note
worker1 <- zookeeper : OK

user -> worker2 : Start H2O node 2
activate worker2
note left
  java -jar h2o-zookeeper.jar water.zookeeper.h2oworker
      -zk a.b.c.d:e
      -zkroot "/zk/path/h2o-uuid"
      -nodes 2
      [plus other H2O options]
end note
worker2 -> h2o2 : Start H2O node 2
activate h2o2
worker2 <- h2o2 : H2O node 2 port chosen
worker2 -> zookeeper : Worker creates ZNode
note left
  create("/zk/path/h2o-uuid/nodes/", (PERSISTENT, SEQUENCE))
  ZNode data: { "ip" : "n2a.n2b.n2c.n2d", "port" : n2e, "pid" : pid2 }
end note
worker2 <- zookeeper : OK

== Poll for nodes started ==

worker1 -> zookeeper : Poll for all nodes started
note left
  getChildren("/zk/path/h2o-uuid/nodes")
end note
worker1 <- zookeeper : OK
note right
  "/zk/path/h2o-uuid/nodes/1"
      { "ip" : "n1a.n1b.n1c.n1d", "port" : n1e, "pid" : pid1 }

  "/zk/path/h2o-uuid/nodes/2"
      { "ip" : "n2a.n2b.n2c.n2d", "port" : n2e, "pid" : pid2 }
end note

worker2 -> zookeeper : Poll for all nodes started
note left
  getChildren("/zk/path/h2o-uuid/nodes")
end note
worker2 <- zookeeper : OK
note right
  (Same as above)
end note

== H2O nodes request flatfile from workers ==

worker1 <- h2o1 : Request flatfile
worker1 <- h2o1 : Worker provides flatfile to H2O

worker2 <- h2o2 : Request flatfile
worker2 -> h2o2 : Worker provides flatfile to H2O

== H2O nodes find each other and notify workers ==

h2o1 -> h2o2 : H2O nodes form a cloud
h2o2 -> h2o1 : H2O nodes form a cloud

worker1 <- h2o1 : cloud size 2
worker2 <- h2o2 : cloud size 2

== Workers create a master (sentinal) ZNode (only one create succeeds) ==

worker1 -> zookeeper : Create cloud ready ZNode
note left
  create("/zk/path/h2o-uuid/master", (PERSISTENT))
  ZNode data: { "ip" : "n1a.n1b.n1c.n1d", "port" : n1e, "pid" : pid1 }
end note
worker1 <- zookeeper : OK
deactivate worker1

worker2 -> zookeeper : Create master ZNode (cloud ready)
note left
  create("/zk/path/h2o-uuid/master", (PERSISTENT))
  ZNode data: { "ip" : "n2a.n2b.n2c.n2d", "port" : n2e, "pid" : pid2 }
end note
worker2 <- zookeeper : KeeperException.NodeExists
note right
  This is OK, exactly one node wins the race
end note
deactivate worker2

== User polls for cloud up ==

user -> driver : User polls for cloud up
activate driver
note left
  java -jar h2o-zookeeper.jar water.zookeeper.h2odriver
      -zk a.b.c.d:e
      -zkroot "/zk/path/h2o-uuid"
      -wait
end note
driver -> zookeeper : Poll for master node ip and port of the new H2O cloud
note left
  getData("/zk/path/h2o-uuid/master")
end note
driver <- zookeeper : OK
note right
  { "ip" : "n1a.n1b.n1c.n1d", "port" : n1e, "pid" : pid1 }  
end note
user <- driver: Master node ip, port
deactivate driver

== User interacts with H2O cloud ==

user -> h2o1 : Point browser to H2O Web UI


@enduml
