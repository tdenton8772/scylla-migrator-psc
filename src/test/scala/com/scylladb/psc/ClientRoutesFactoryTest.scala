package com.scylladb.psc

import java.net.InetSocketAddress

import com.datastax.spark.connector.cql.{
  CloudBasedContactInfo,
  IpBasedContactInfo,
  NoAuthConf,
  ProfileFileBasedContactInfo
}
import org.apache.spark.SparkConf

import scala.jdk.CollectionConverters._

/** Tests the routing decision, which is where the risk lives. Opening a session needs a live
  * cluster; deciding whether a session should get client routes does not, and getting that
  * decision wrong is what breaks a migration.
  */
class ClientRoutesFactoryTest extends munit.FunSuite {

  private val ConnId = "037d7155-d76f-5244-ac7d-2993b44bab16"
  private val ConnId2 = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  private val PscHost = "scylla-psc-nr67015-728978d2.clusters.scylla.cloud"

  private def conf(entries: (String, String)*): SparkConf =
    entries.foldLeft(new SparkConf(false)) { case (c, (k, v)) => c.set(k, v) }

  private def ip(hosts: String*): IpBasedContactInfo =
    IpBasedContactInfo(hosts.map(h => new InetSocketAddress(h, 9042)).toSet, NoAuthConf)

  private val withId = conf("spark.scylla.psc.connectionId" -> ConnId)

  // ---------------------------------------------------------------------------
  // Secure connect bundles are never given client routes.
  //
  // The driver throws when a session has both, so this must hold no matter how the rest is
  // configured -- it is what keeps an Astra source on the same code path as the stock migrator.
  // ---------------------------------------------------------------------------

  test("a bundle session never gets client routes") {
    val d = ClientRoutesFactory.decide(CloudBasedContactInfo("/opt/astra.zip", NoAuthConf), withId)
    assert(clue(d).isInstanceOf[ClientRoutesFactory.PlainSession])
  }

  test("a bundle session is skipped even when its host is listed in psc.hosts") {
    // The bundle case is matched before hosts are consulted; no host configuration can reach it.
    val c = conf(
      "spark.scylla.psc.connectionId" -> ConnId,
      "spark.scylla.psc.hosts"        -> s"$PscHost,/opt/astra.zip"
    )
    val d = ClientRoutesFactory.decide(CloudBasedContactInfo("/opt/astra.zip", NoAuthConf), c)
    assert(clue(d).isInstanceOf[ClientRoutesFactory.PlainSession])
  }

  test("a profile-file session never gets client routes") {
    val d = ClientRoutesFactory.decide(ProfileFileBasedContactInfo("/opt/profile.conf"), withId)
    assert(clue(d).isInstanceOf[ClientRoutesFactory.PlainSession])
  }

  // ---------------------------------------------------------------------------
  // Plain clusters
  // ---------------------------------------------------------------------------

  test("with no hosts configured, a plain cluster gets client routes") {
    ClientRoutesFactory.decide(ip(PscHost), withId) match {
      case ClientRoutesFactory.WithClientRoutes(routes) =>
        assertEquals(routes.getEndpoints.asScala.map(_.getConnectionId).toList, List(ConnId))
      case other => fail(s"expected client routes, got $other")
    }
  }

  test("with hosts configured, only a matching cluster gets client routes") {
    val c = conf("spark.scylla.psc.connectionId" -> ConnId, "spark.scylla.psc.hosts" -> PscHost)
    assert(ClientRoutesFactory.decide(ip(PscHost), c).isInstanceOf[ClientRoutesFactory.WithClientRoutes])
  }

  test("a non-matching cluster is skipped -- the Cassandra-source case") {
    // Regression: without this, a Cassandra source is given client routes, queries
    // system.client_routes on a server that has no such table, and the migration dies with
    // "Server does not support CLIENT_ROUTES_CHANGE".
    val c = conf("spark.scylla.psc.connectionId" -> ConnId, "spark.scylla.psc.hosts" -> PscHost)
    val d = ClientRoutesFactory.decide(ip("cassandra-0.cassandra.cass.svc.cluster.local"), c)
    d match {
      case ClientRoutesFactory.PlainSession(reason) => assert(reason.contains("not listed"), reason)
      case other                                    => fail(s"expected a plain session, got $other")
    }
  }

  test("host matching is case-insensitive") {
    val c = conf(
      "spark.scylla.psc.connectionId" -> ConnId,
      "spark.scylla.psc.hosts"        -> PscHost.toUpperCase
    )
    assert(ClientRoutesFactory.decide(ip(PscHost), c).isInstanceOf[ClientRoutesFactory.WithClientRoutes])
  }

  test("no connection id means no client routes, and the reason names the key") {
    ClientRoutesFactory.decide(ip(PscHost), new SparkConf(false)) match {
      case ClientRoutesFactory.PlainSession(reason) =>
        assert(reason.contains("spark.scylla.psc.connectionId"), reason)
      case other => fail(s"expected a plain session, got $other")
    }
  }

  // ---------------------------------------------------------------------------
  // Building the driver configuration
  // ---------------------------------------------------------------------------

  test("several connection ids, one per availability zone") {
    val c = conf("spark.scylla.psc.connectionId" -> s" $ConnId , $ConnId2 ")
    val routes = ClientRoutesFactory.clientRoutesConfig(c).getOrElse(fail("expected a config"))
    assertEquals(routes.getEndpoints.asScala.map(_.getConnectionId).toList, List(ConnId, ConnId2))
  }

  test("a repeated connection id is collapsed rather than rejected by the driver") {
    val c = conf("spark.scylla.psc.connectionId" -> s"$ConnId,$ConnId")
    val routes = ClientRoutesFactory.clientRoutesConfig(c).getOrElse(fail("expected a config"))
    assertEquals(routes.getEndpoints.size, 1)
  }

  test("connectionAddr overrides the address from system.client_routes") {
    val c = conf(
      "spark.scylla.psc.connectionId"   -> ConnId,
      "spark.scylla.psc.connectionAddr" -> "10.128.0.36"
    )
    val routes = ClientRoutesFactory.clientRoutesConfig(c).getOrElse(fail("expected a config"))
    assertEquals(routes.getEndpoints.asScala.head.getConnectionAddrOverride, "10.128.0.36")
  }

  test("shard awareness is off unless explicitly enabled") {
    // Through a load balancer the driver cannot target a shard by local source port, so this
    // stays off unless the operator confirms Proxy Protocol v2 is configured end to end.
    val routes = ClientRoutesFactory
      .clientRoutesConfig(conf("spark.scylla.psc.connectionId" -> ConnId))
      .getOrElse(fail("expected a config"))
    assertEquals(routes.isShardAwarenessEnabled, false)
  }

  test("shard awareness can be opted into") {
    val routes = ClientRoutesFactory
      .clientRoutesConfig(
        conf(
          "spark.scylla.psc.connectionId"   -> ConnId,
          "spark.scylla.psc.shardAwareness" -> "true"
        )
      )
      .getOrElse(fail("expected a config"))
    assertEquals(routes.isShardAwarenessEnabled, true)
  }

  test("an empty connection id is treated as unset") {
    assertEquals(ClientRoutesFactory.clientRoutesConfig(conf("spark.scylla.psc.connectionId" -> "  ")), None)
  }
}
