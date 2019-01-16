name := "genner-poc-ravi"

version := "0.1"

scalaVersion := "2.12.8"

//resolvers += "Local Maven" at Path.userHome.asFile.toURI.toURL + ".m2/repository"

libraryDependencies += "com.google.code.gson" % "gson" % "2.8.5"

libraryDependencies += "com.github.azakordonets" %% "fabricator" % "2.1.5"

//libraryDependencies += "com.gaurav.kafka" %% "kakfa-producer-consumer-example" % "0.0.1-SNAPSHOT"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.0.0"