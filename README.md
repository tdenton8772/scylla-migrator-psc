# ScyllaDB Migrator over Private Service Connect

A small Spark connection factory that lets
[scylla-migrator](https://github.com/scylladb/scylla-migrator) reach a ScyllaDB
Cloud cluster through a private endpoint - GCP Private Service Connect or AWS
PrivateLink - using the Java driver's client-routes feature.

One class, no changes to the migrator itself. Configured entirely with
`spark-submit` flags.

Needs ScyllaDB Enterprise 2026.1 or later on the target cluster.

## Build

`psc-factory.jar` is checked in for convenience. To build it yourself:

    sbt package     # -> target/scala-2.13/psc-factory_2.13-0.1.0.jar
    sbt test        # 12 tests covering the routing decision

## 1. Get your connection id

Run from a host inside your VPC:

    cqlsh <PRIVATE_ENDPOINT_IP> 9000 -u <USER> -p <PASSWORD> \
      -e "SELECT connection_id, address, port FROM system.client_routes;"

Copy the `connection_id` value. It is a UUID.

> Do not use the connection ID shown in the ScyllaDB Cloud console, or the
> `pscConnectionId` from `gcloud`. Both are different numbers and neither works.
> Only the value from this table is correct.

If the table does not exist, your cluster is older than 2026.1. Stop here.

---

## 2. Build the migrator

The released migrator ships a Java driver that predates this feature, so it
needs one rebuild. In `build.sbt`, add:

    val javaDriverVersion = "4.19.2.1"

and inside the existing `inThisBuild(List(...))` block, add:

    dependencyOverrides ++= Seq(
      "com.scylladb" % "java-driver-core-shaded"      % javaDriverVersion,
      "com.scylladb" % "java-driver-mapper-runtime"   % javaDriverVersion,
      "com.scylladb" % "java-driver-mapper-processor" % javaDriverVersion
    )

Then:

    sbt migrator/assembly

No other changes to the migrator.

---

## 3. Point config.yaml at the endpoint

Use the private endpoint hostname and a discovery port (9000-9002). Not 9042.

    target:
      type: scylla
      host: <your-endpoint>.clusters.scylla.cloud
      port: 9000
      ...

---

## 4. Add the jar and three flags to spark-submit

    --jars psc-factory.jar \
    --conf spark.cassandra.connection.factory=com.scylladb.psc.ClientRoutesFactory \
    --conf spark.scylla.psc.connectionId=<YOUR_CONNECTION_ID>

If your SOURCE is a plain Cassandra cluster (not Astra), add a fourth flag so
that only the target uses the private endpoint:

    --conf spark.scylla.psc.hosts=<your-endpoint>.clusters.scylla.cloud

Not needed when the source is Astra - a secure connect bundle is detected
automatically and left alone.

---

## 5. Check that it worked

**Exit code 0 does not mean it worked.** A misconfigured run copies every row
and exits cleanly, while quietly using one node instead of all of them.

Check the log:

    grep -c 'Error while opening new channel' <log>      # must be 0
    grep -oE ':900[0-9]' <log> | sort | uniq -c          # must show per-node ports

A good run shows several different ports (9003, 9004, 9005 - one per node).
A bad run shows only the discovery port 9000, plus connection errors to
internal addresses that your VPC cannot reach.

On Kubernetes, check the executor pods, not just the driver.

---

## Deploying on GKE

For running this as a Spark job on GKE with YuniKorn, see
[DEPLOY-GKE.md](DEPLOY-GKE.md).

## Notes

- TLS is not available over the private endpoint today (`tls_port` is null).
  Do not set `sslOptions`.
- `spark.scylla.psc.connectionId` accepts a comma-separated list if your cluster
  is fronted by one endpoint per availability zone.
- `spark.scylla.psc.connectionAddr` overrides the address from
  `system.client_routes` if in-VPC DNS does not resolve the cluster hostname.
- The property names must start with `spark.` - Spark only sends properties in
  that namespace to the executors.

## License

Apache License 2.0.
