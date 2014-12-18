###Spray json protocol for Cassandra Row and ResultSet

Marshal Row and ResultSet to json

#### Dependencies

Add to build.sbt :
```
scalaVersion := "2.11.2"

libraryDependencies ++= Seq (
"io.spray" %% "spray-can" % "1.3.2",
"io.spray" %% "spray-json" % "1.3.0",
"com.datastax.cassandra" % "cassandra-driver-core" % "2.1.3")
```

####Usage
```
import CassandraJsonProtocol._
```
