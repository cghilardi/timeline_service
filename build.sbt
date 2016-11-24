name := "quickstart"

version := "1.0"

scalaVersion in ThisBuild := "2.11.8"

mainClass in Compile := Some("Timeline")

resolvers ++= Seq(
  "Twitter repository" at "http://maven.twttr.com",
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-http" % "6.38.0",
  "com.twitter" %% "finagle-thrift" % "6.38.0",
  "com.twitter" %% "finagle-redis" % "6.38.0",
  "com.twitter" %% "scrooge-core" % "4.11.0",
  "org.apache.thrift" % "libthrift" % "0.9.3",
  "com.typesafe.play" %% "play-json" % "2.3.4"
)

lazy val app = project.in(file("."))
  .settings(
  scroogeThriftSourceFolder in Compile <<= baseDirectory {
    base => base / ""
  }
)

assemblySettings
