# akka-persistence-ignite (Java API)
A _**journal**_ and _**snapshot**_  store plugin for akka-persistence using **_Ignite_**.


#How to use 
Enable plugins
````buildoutcfg
akka.persistence.journal.plugin = "akka.persistence.journal.ignite"
akka.persistence.snapshot-store.plugin = "akka.persistence.snapshot.ignite"
````

Configure Ignite if need, default configured on localhost. 
````buildoutcfg
akka.persistence.ignite.config-file = "ignite-config.xml"
````
