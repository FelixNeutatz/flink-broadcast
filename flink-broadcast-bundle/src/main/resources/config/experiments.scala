package config

import com.samskivert.mustache.Mustache
import com.typesafe.config.ConfigFactory
import de.tu_berlin.dima.flink.beans.experiment.FlinkExperiment
import org.peelframework.core.beans.data.{DataSet, ExperimentOutput}
import org.peelframework.core.beans.experiment.ExperimentSuite
import org.peelframework.core.beans.system.Lifespan
import org.peelframework.dstat.beans.system.Dstat
import org.peelframework.flink.beans.system.Flink
import org.peelframework.hadoop.beans.system.HDFS2
import org.springframework.context.annotation._
import org.springframework.context.{ApplicationContext, ApplicationContextAware}

/** Experiments definitions for the 'flink-hashagg' bundle. */
@Configuration
@ComponentScan(// Scan for annotated Peel components in the 'de.tu_berlin.dima.experiments.flink.hashagg' package
  value = Array("de.tu_berlin.dima.experiments.flink.broadcast"),
  useDefaultFilters = false,
  includeFilters = Array[ComponentScan.Filter](
    new ComponentScan.Filter(value = Array(classOf[org.springframework.stereotype.Service])),
    new ComponentScan.Filter(value = Array(classOf[org.springframework.stereotype.Component]))
  )
)
@ImportResource(value = Array(
  "classpath:peel-core.xml",
  "classpath:peel-extensions.xml"
))
@Import(value = Array(
  classOf[org.peelframework.extensions] // base system beans
))
class experiments extends ApplicationContextAware {

  val runs = 3

  /* The enclosing application context. */
  var ctx: ApplicationContext = null

  def setApplicationContext(ctx: ApplicationContext): Unit = {
    this.ctx = ctx
  }

  // ---------------------------------------------------
  // Systems
  // ---------------------------------------------------

  @Bean(name = Array("flink-1.0.3"))
  def `flink-1.0.3`: Flink = new Flink(
    //@formatter:off
    version      = "1.0.3",
    configKey    = "flink",
    lifespan     = Lifespan.EXPERIMENT,
    dependencies = Set(ctx.getBean("hdfs-2.7.1", classOf[HDFS2])),
    mc           = ctx.getBean(classOf[Mustache.Compiler])
    //@formatter:on
  )

  // ---------------------------------------------------
  // Output Paths
  // ---------------------------------------------------

  def hdfsOutput(path: String): ExperimentOutput = new ExperimentOutput(
    //@formatter:off
    path = s"$${system.hadoop-2.path.output}/$path",
    fs   = ctx.getBean("hdfs-2.7.1", classOf[HDFS2])
    //@formatter:on
  )

  // ---------------------------------------------------
  // Experiments
  // ---------------------------------------------------

  @Bean(name = Array("broadcast"))
  def broadcast: ExperimentSuite = new ExperimentSuite(
    for {
      numberOfTaskSlots <- Seq(1, 2, 4, 8, 16)
    } yield new FlinkExperiment(
      //@formatter:off
      name         = f"broadcast.$numberOfTaskSlots%02d",
      command      =
        s"""
           |-v -c de.tu_berlin.dima.experiments.flink.broadcast.BroadcastJob \\
           |-p $$(($numberOfTaskSlots * $${env.slaves.all.total.hosts})) \\
           |$${app.path.apps}/flink-broadcast-flink-jobs-1.0-SNAPSHOT.jar \\
           |$$(($numberOfTaskSlots * $${env.slaves.all.total.hosts})) \\
           |10 \\
           |${hdfsOutput("result").path}
        """.stripMargin.trim,
      config     = ConfigFactory.parseString(
        s"""
           |system.flink.config.yaml {
           |  taskmanager.numberOfTaskSlots = ${numberOfTaskSlots}
           |}
         """.stripMargin),
      runs         = runs,
      runner       = ctx.getBean("flink-1.0.3", classOf[Flink]),
      systems      = Set(ctx.getBean("dstat-0.7.2", classOf[Dstat])),
      inputs       = Set.empty[DataSet],
      outputs      = Set(hdfsOutput("result"))
      //@formatter:on
    )
  )
}
