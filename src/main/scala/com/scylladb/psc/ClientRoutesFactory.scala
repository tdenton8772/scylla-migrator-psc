package com.scylladb.psc

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.config.{
  ClientRouteProxy,
  ClientRoutesConfig,
  DefaultDriverOption,
  DriverConfigLoader
}
import com.datastax.spark.connector.cql.{
  CassandraConnectionFactory,
  CassandraConnectorConf,
  CloudBasedContactInfo,
  ContactInfo,
  DefaultConnectionFactory,
  IpBasedContactInfo,
  MultiplexingSchemaListener,
  ProfileFileBasedContactInfo,
  Scanner
}
import com.datastax.spark.connector.rdd.ReadConf
import org.apache.spark.{ SparkConf, SparkEnv }
import org.slf4j.LoggerFactory

/** Adds the Java driver's client-routes feature (AWS PrivateLink, Azure Private Link, GCP Private
  * Service Connect) to a stock scylla-migrator, with no change to the migrator itself.
  *
  * Wire it up entirely through Spark configuration:
  *
  * {{{
  * --jars psc-factory.jar
  * --conf spark.cassandra.connection.factory=com.scylladb.psc.ClientRoutesFactory
  * --conf spark.scylla.psc.connectionId=<connection id from system.client_routes>
  * }}}
  *
  * ==Why this exists rather than a HOCON file==
  *
  * Client routes can also be configured in HOCON, under
  * `datastax-java-driver.advanced.client-routes`. But the driver reads that from the default
  * profile of ''every'' session the JVM builds, and it cannot be narrowed to one cluster. A
  * migration whose source is Astra therefore breaks: the source session gets both a secure connect
  * bundle and client routes, and `DefaultDriverContext.validateClientRoutesConfiguration` throws
  * `IllegalStateException` because the two are mutually exclusive.
  *
  * A connection factory sees each session individually, so it can decide per cluster. The rule
  * here needs no extra configuration: '''a session using a secure connect bundle is left alone''',
  * everything else gets client routes. For an Astra-to-ScyllaDB-Cloud migration that is exactly
  * right — the Astra side keeps its bundle, the ScyllaDB side goes through the private endpoint.
  *
  * ==Requirements==
  *
  *   - java-driver >= 4.19.0.9 in the migrator assembly (the released v2.1.5 ships 4.19.0.4)
  *   - ScyllaDB Enterprise >= 2026.1 on the private-endpoint cluster
  *   - the property name must begin with `spark.` so that it reaches the executors
  */
object ClientRoutesFactory extends CassandraConnectionFactory {

  /** Must start with `spark.`; Spark only propagates properties in that namespace to executors. */
  private val ConnectionIdKey = "spark.scylla.psc.connectionId"
  private val ConnectionAddrKey = "spark.scylla.psc.connectionAddr"

  /** Which cluster the private endpoint belongs to, as a comma-separated list of the contact-point
    * hosts configured for it.
    *
    * Optional. Leave it unset when every non-Astra cluster in the job is behind the endpoint —
    * both sides on the same endpoint, or an Astra source (which is recognised by its secure
    * connect bundle) paired with a private-endpoint target.
    *
    * Set it when the job talks to two plain clusters and only one is behind the endpoint, e.g. a
    * self-managed Cassandra source migrating to ScyllaDB Cloud. Without it the source session also
    * gets client routes, queries `system.client_routes` on a server that has no such table, and
    * fails with "Server does not support CLIENT_ROUTES_CHANGE".
    */
  private val HostsKey = "spark.scylla.psc.hosts"

  /** Opt in to shard awareness through the endpoint. Off by default, and it should stay off
    * unless the whole path supports it.
    *
    * The driver targets a shard by binding a particular local source port, which ScyllaDB reads to
    * route the connection. A load balancer rewrites that port, so ScyllaDB picks a different shard
    * than the driver asked for and logs:
    *
    * {{{
    * New channel ... connected to shard 1, but shard 0 was requested.
    * }}}
    *
    * Setting this to true is only correct when the load balancer forwards the client's original
    * source address with Proxy Protocol v2 and ScyllaDB is configured to accept it. Without that,
    * leave it off: the factory then disables the driver's own shard-aware port binding, which
    * stops the warning and the reconnection churn behind it.
    */
  private val ShardAwarenessKey = "spark.scylla.psc.shardAwareness"

  @transient private lazy val log = LoggerFactory.getLogger(getClass)

  override def createSession(conf: CassandraConnectorConf): CqlSession = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    decide(conf.contactInfo, sparkConf) match {
      case PlainSession(reason) =>
        log.info("Opening this session without client routes: {}", reason)
        DefaultConnectionFactory.createSession(conf)

      case WithClientRoutes(routes) =>
        // INFO, because a session opening correctly is not a warning. Note that some Spark log
        // configurations filter INFO for loggers outside org.apache.spark and the migrator's own
        // packages, in which case this line will not appear -- verify with the per-node ports in
        // the log instead, which do not depend on log configuration.
        log.info(
          "Opening a CQL session through client routes, connection id(s): {}",
          routes.getEndpoints.toString
        )
        // Reuse the connector's own option builder so every pool size, timeout, retry and TLS
        // setting it derives from CassandraConnectorConf still applies.
        val shardAwareness = sparkConf.getBoolean(ShardAwarenessKey, defaultValue = false)
        val builder =
          DefaultConnectionFactory.connectorConfigBuilder(conf, DriverConfigLoader.programmaticBuilder())
        val configLoader =
          (if (shardAwareness) builder
           else
             // Shard-aware port binding cannot survive the load balancer's source-port rewrite.
             // Leaving it on (the driver's default) makes every pooled connection land on an
             // unintended shard and be retried.
             builder.withBoolean(
               DefaultDriverOption.CONNECTION_ADVANCED_SHARD_AWARENESS_ENABLED,
               false
             )).build()

        val appName = Option(SparkEnv.get).map(_.conf.getAppId).getOrElse("NoAppID")
        val ipConf = conf.contactInfo.asInstanceOf[IpBasedContactInfo]

        ipConf.authConf.authProvider
          .fold(CqlSession.builder())(CqlSession.builder().withAuthProvider)
          .withConfigLoader(configLoader)
          .withClientRoutesConfig(routes)
          .withApplicationName(s"Spark-Cassandra-Connector-$appName")
          .withSchemaChangeListener(new MultiplexingSchemaListener())
          .build()
    }
  }

  /** What to do with one session. Separated from [[createSession]] so the decision can be tested
    * without a reachable cluster -- opening a session requires a live server, the decision does
    * not, and the decision is the part that carries the risk.
    */
  private[psc] sealed trait Decision
  private[psc] case class PlainSession(reason: String) extends Decision
  private[psc] case class WithClientRoutes(config: ClientRoutesConfig) extends Decision

  /** The whole routing rule, as a pure function of the session's contact info and the Spark
    * configuration.
    *
    * Order matters. Secure connect bundles are matched first and unconditionally: the driver
    * throws if a bundle session is also given client routes, so no host configuration may reach
    * one. That keeps an Astra source on exactly the code path the stock migrator uses.
    */
  private[psc] def decide(contactInfo: ContactInfo, sparkConf: SparkConf): Decision =
    contactInfo match {
      case CloudBasedContactInfo(path, _) =>
        PlainSession(s"secure connect bundle in use ($path)")

      case _: ProfileFileBasedContactInfo =>
        // This option makes the connector discard all programmatic configuration, so there is
        // nothing useful to add. The profile file can carry advanced.client-routes itself.
        PlainSession("a driver profile file is in use")

      case ipConf: IpBasedContactInfo =>
        if (!isPrivateEndpointCluster(ipConf, sparkConf))
          PlainSession(
            s"contact points ${ipConf.hosts.map(_.getHostString).mkString(",")} are not listed in '$HostsKey'"
          )
        else
          clientRoutesConfig(sparkConf) match {
            case Some(routes) => WithClientRoutes(routes)
            case None         => PlainSession(s"'$ConnectionIdKey' is not set")
          }
    }

  /** Does this session target the cluster behind the private endpoint?
    *
    * With no `spark.scylla.psc.hosts` configured every non-bundle session qualifies, which is the
    * right default for the common shapes. When it is configured, only sessions whose contact
    * points match are given client routes.
    */
  private[psc] def isPrivateEndpointCluster(
    ipConf: IpBasedContactInfo,
    sparkConf: SparkConf
  ): Boolean =
    configuredHosts(sparkConf) match {
      case Nil => true
      case hosts =>
        val contactPoints = ipConf.hosts.map(_.getHostString.toLowerCase)
        hosts.exists(h => contactPoints.contains(h.toLowerCase))
    }

  private[psc] def configuredHosts(sparkConf: SparkConf): List[String] = {
    sparkConf
      .getOption(HostsKey)
      .map(_.split(",").map(_.trim).filter(_.nonEmpty).toList)
      .getOrElse(Nil)
  }

  /** Read the connection id from the ambient Spark configuration.
    *
    * Resolved at session-creation time rather than construction time: this object is instantiated
    * reflectively by the connector, so there is nowhere to pass configuration in, and on an
    * executor the SparkEnv is only meaningful once the executor is running.
    */
  private[psc] def clientRoutesConfig(sparkConf: SparkConf): Option[ClientRoutesConfig] = {
    sparkConf
      .getOption(ConnectionIdKey)
      .map(_.trim)
      .filter(_.nonEmpty)
      .map { ids =>
        val addr = sparkConf.getOption(ConnectionAddrKey).map(_.trim).filter(_.nonEmpty)
        val builder = ClientRoutesConfig.builder()
        builder.withShardAwareness(sparkConf.getBoolean(ShardAwarenessKey, defaultValue = false))
        // A comma-separated list covers a cluster fronted by one endpoint per availability zone.
        ids.split(",").map(_.trim).filter(_.nonEmpty).distinct.foreach { id =>
          builder.addEndpoint(new ClientRouteProxy(id, addr.orNull))
        }
        builder.build()
      }
  }

  /** Defer to the connector so we use whatever scanner it would have picked for this cluster. */
  override def getScanner(
    readConf: ReadConf,
    connConf: CassandraConnectorConf,
    columnNames: IndexedSeq[String]
  ): Scanner =
    DefaultConnectionFactory.getScanner(readConf, connConf, columnNames)
}
