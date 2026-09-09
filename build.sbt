name         := "psc-factory"
organization := "com.scylladb"
version      := "0.1.0"
scalaVersion := "2.13.14"
scalacOptions ++= Seq("-release:17", "-deprecation", "-feature")

// Everything here is already inside the migrator assembly at runtime; this jar
// carries one class and nothing else.
libraryDependencies ++= Seq(
  "com.scylladb"     %% "spark-scylladb-connector" % "4.1.4" % Provided,
  "org.apache.spark" %% "spark-core"               % "4.0.2" % Provided,
  "org.apache.spark" %% "spark-sql"                % "4.0.2" % Provided,
  "org.scalameta"    %% "munit"                    % "1.0.1" % Test
)

// Spark on the test classpath trips sbt's layered test classloader; fork instead, as the
// migrator's own test project does.
Test / fork := true
Test / javaOptions ++= Seq("--add-exports", "java.base/sun.nio.ch=ALL-UNNAMED")
