/*
* Queue support for dispatching jobs to Arvados.
*
* Copyright (c) 2015 Curoverse, Inc.
*
* Based on code Copyright (c) 2012 The Broad Institute
*
* Permission is hereby granted, free of charge, to any person
* obtaining a copy of this software and associated documentation
* files (the "Software"), to deal in the Software without
* restriction, including without limitation the rights to use,
* copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the
* Software is furnished to do so, subject to the following
* conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
* OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
* HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
* WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
* THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package org.broadinstitute.gatk.queue.engine.arvados

import org.broadinstitute.gatk.queue.QException
import org.broadinstitute.gatk.queue.util.{Logging,Retry}
import org.broadinstitute.gatk.queue.function.CommandLineFunction
import org.broadinstitute.gatk.queue.engine.{RunnerStatus, CommandLineJobRunner}
import java.util.{Date, Collections, HashMap, ArrayList, Map}
import org.arvados.sdk.java.Arvados
import org.json.simple.JSONArray
import org.json.simple.JSONObject
import java.nio.file.{Files, Paths}
import com.google.api.client.http
import org.broadinstitute.gatk.utils.runtime.{ProcessSettings, OutputStreamSettings, ProcessController}
import java.io
import java.nio.charset.Charset
import java.io.PrintWriter

/**
 * Runs jobs using Arvados.
 */
class ArvadosJobRunner(val arv: Arvados,
                       val jobs: scala.collection.mutable.Map[String, String],
                       val function: CommandLineFunction)
extends CommandLineJobRunner with Logging {

  /** Job Id of the currently executing job. */
  var jobUuid: String = _
  override def jobIdString = jobUuid
  var outfilePath: String = ""
  var outfileName: String = ""
  var workdir: String = ""

  def start() {
    arv.synchronized {
      val body = new HashMap[String, Object]()
      body.put("script", "run-command")
      body.put("script_version", "master")
      body.put("repository", "arvados")

      val runtime = new HashMap[String, Object]()
      runtime.put("docker_image", "arvados/jobs-java-bwa-samtools")
      runtime.put("max_tasks_per_node", 1:java.lang.Integer)
      body.put("runtime_constraints", runtime)

      var cl = function.commandLine

      {
        // Adjust tmpdir
        val rx = """'-Djava.io.tmpdir=([^']+)'""".r
        cl = rx.replaceFirstIn(cl, "'-Djava.io.tmpdir=\\$(task.tmpdir)'")
      }

      {
        // Adjust thread count
        val rx = """'-nct' '(\d+)'""".r
        cl = rx.replaceFirstIn(cl, "'-nct' '\\$(node.cores)'")
      }

      val hap = """.*'org.broadinstitute.gatk.engine.CommandLineGATK'.*'-T' 'HaplotypeCaller'.*""".r
      val cat = """.*'org.broadinstitute.gatk.tools.CatVariants'.*""".r

      var vwdpdh: Option[String] = None

      cl match {
        case hap() => {
          // HaplotypeCaller support

          {
            // Capture and adjust output path
            val rx = """'-o' '([^']+/\.queue/scatterGather/([^/]+/[^/]+)/([^']+))'""".r
            rx.findFirstMatchIn(cl) match {
              case Some(m) => {
                outfilePath = m.group(1)
                workdir = m.group(2)
                outfileName = m.group(3)
              }
              case None => {}
            }
            cl = rx.replaceFirstIn(cl, "'-o' '$3'")
          }

          {
            // Capture and adjust scatter intervals
            val rx = """'-L' '([^']+/\.queue/scatterGather/[^/]+/[^/]+/)([^']+)'""".r

            rx.findFirstMatchIn(cl) match {
              case Some(m) => {
                // Need to shell out to arv-put to upload scatterdir
                val commandLine = Array("arv-put", "--no-progress", "--portable-data-hash", m.group(1))
                val stdoutSettings = new OutputStreamSettings
                val stderrSettings = new OutputStreamSettings

                val temp = java.io.File.createTempFile("PDH", ".tmp");

                stdoutSettings.setOutputFile(temp, true)

                val processSettings = new ProcessSettings(
                  commandLine, false, function.commandDirectory, null,
                  null, stdoutSettings, stderrSettings)

                val controller = ProcessController.getThreadLocal
                val exitStatus = controller.exec(processSettings).getExitValue
                if (exitStatus != 0) {
                  updateStatus(RunnerStatus.FAILED)
                  return
                }

                vwdpdh = Some(Files.readAllLines(Paths.get(temp.getPath()), Charset.defaultCharset()).get(0))

                temp.delete()
              }
              case None => {}
            }

            cl = rx.replaceFirstIn(cl, "'-L' '$2'")
          }
        }
        case cat() => {
          // CatVariants support

          {
            // Capture and adjust output path
            val rx = """'-out' '([^']+/([^/']+))'""".r
            rx.findFirstMatchIn(cl) match {
              case Some(m) => {
                outfilePath = m.group(1)
                workdir = ""
                outfileName = m.group(2)
              }
              case None => {}
            }
            cl = rx.replaceFirstIn(cl, "'-out' '$2'")
          }
          {
            val rx = """'-V' '[^']+/\.queue/scatterGather/([^/]+/[^/]+)/([^']+)'""".r
            for (rx(work, file) <- rx findAllIn cl) {
              jobs.get(work) match {
                case Some(d) => {
                  cl = rx.replaceFirstIn(cl, "'-V' '/keep/" + d + "/" + file + "'")
                }
                case None => {}
              }
            }
          }
        }
        case _ => {
          throw new QException("Did not recognize tool command line, only supports HaplotypeCaller and CatVariants.")
        }
      }

      val parameters = new HashMap[String, Object]()
      val cmdLine = new ArrayList[String]
      cmdLine.add("/bin/sh")
      cmdLine.add("-c")
      cmdLine.add(cl)
      parameters.put("command", cmdLine)
      vwdpdh match {
        case Some(vwd) => parameters.put("task.vwd", vwd)
        case None => {}
      }

      body.put("script_parameters", parameters)

      val json = new JSONObject(body)
      val p = new HashMap[String, Object]()
      p.put("job", json.toString())
      p.put("find_or_create", "true")
      var response: Option[java.util.Map[_, _]] = None

      var retry = 3
      while (retry > 0) {
        try {
          response = Some(arv.call("jobs", "create", p))
          retry = 0
        } catch {
          case e: java.net.SocketTimeoutException => {
            retry -= 1
          }
        }
      }

      response match {
        case Some(r) => {
          jobUuid = r.get("uuid").asInstanceOf[String]
          println("Queued job " + jobUuid)
          updateStatus(RunnerStatus.RUNNING)
        }
        case None => {
          throw new QException("Job creation failed.")
        }
      }
    }
  }

  def updateJobStatus() = {
    arv.synchronized {
      val p = new HashMap[String, Object]()
      p.put("uuid", jobUuid)
      val response = arv.call("jobs", "get", p)

      var returnStatus: RunnerStatus.Value = null
      val state = response.get("state")

      state match {
        case "Queued" => returnStatus = RunnerStatus.RUNNING
        case "Running" => returnStatus = RunnerStatus.RUNNING
        case "Complete" => {
          jobs += (workdir -> response.get("output").asInstanceOf[String])

          Files.createSymbolicLink(Paths.get(outfilePath + ".idx"), Paths.get("/keep/" + response.get("output") + "/" + outfileName + ".idx"))

          val writer = new PrintWriter(function.jobOutputFile.getPath, "UTF-8")
          writer.println("Job log for " + jobUuid + " in " + response.get("log") + "/" + response.get("uuid") + ".log.txt")
          writer.close()

          Files.createSymbolicLink(Paths.get(outfilePath), Paths.get("/keep/" + response.get("output") + "/" + outfileName))
          returnStatus = RunnerStatus.DONE

          println("Job " + jobUuid + " completed")
        }
        case "Failed" => returnStatus = RunnerStatus.FAILED
        case "Cancelled" => returnStatus = RunnerStatus.FAILED
      }

      updateStatus(returnStatus)

      true
    }
  }

  def tryStop() {
    try {
      val p = new HashMap[String, Object]()
      p.put("uuid", jobUuid)
      p.put("job", "")
      val response = arv.call("jobs", "cancel", p)
    } catch {
      case e: com.google.api.client.http.HttpResponseException => {}
    }
  }
}
