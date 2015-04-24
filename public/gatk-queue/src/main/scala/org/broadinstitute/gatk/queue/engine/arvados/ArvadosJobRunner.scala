/*
* Copyright (c) 2012 The Broad Institute
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
import java.util.{Date, Collections, HashMap, ArrayList}
import org.arvados.sdk.java.Arvados
import org.json.simple.JSONArray
import org.json.simple.JSONObject
import java.nio.file.{Files, Paths}
import com.google.api.client.http
import org.broadinstitute.gatk.utils.runtime.{ProcessSettings, OutputStreamSettings, ProcessController}
import java.io
import java.nio.charset.Charset

/**
 * Runs jobs using Arvados.
 */
class ArvadosJobRunner(val arv: Arvados, val function: CommandLineFunction) extends CommandLineJobRunner with Logging {
  /** Job Id of the currently executing job. */
  var jobUuid: String = _
  override def jobIdString = jobUuid
  var outfilePath: String = ""
  var outfileName: String = ""

  // Set the display name to < 512 characters of the description
  // NOTE: Not sure if this is configuration specific?
  protected val jobNameLength = 500
  protected val jobNameFilter = """[^A-Za-z0-9_]"""
  protected def functionNativeSpec = function.jobNativeArgs.mkString(" ")

  def start() {
    arv.synchronized {
      var body = new HashMap[String, Object]()
      body.put("script", "run-command")
      body.put("script_version", "master")
      body.put("repository", "arvados")

      var runtime = new HashMap[String, Object]()
      runtime.put("docker_image", "arvados/jobs-java-bwa-samtools")
      runtime.put("max_tasks_per_node", 1:java.lang.Integer)
      body.put("runtime_constraints", runtime)

      var cl = function.commandLine

      // Adjust tmpdir
      var rx = """'-Djava.io.tmpdir=([^']+)'""".r
      cl = rx.replaceFirstIn(cl, "'-Djava.io.tmpdir=\\$(task.tmpdir)'")

      // Adjust thread count
      rx = """'-nct' '(\d+)'""".r
      cl = rx.replaceFirstIn(cl, "'-nct' '\\$(node.cores)'")

      // Capture and adjust output path
      rx = """'-o' '([^']+/\.queue/scatterGather/[^/]+/[^/]+/([^']+))'""".r
      rx.findFirstMatchIn(cl) match {
        case Some(m) => {
          outfilePath = m.group(1)
          outfileName = m.group(1)
        }
        case None => {}
      }
      cl = rx.replaceFirstIn(cl, "'-o' '$2'")

      // Capture and adjust scatter intervals
      rx = """'-L' '([^']+/\.queue/scatterGather/[^/]+/[^/]+/)([^']+)'""".r

      var vwdpdh = ""
      rx.findFirstMatchIn(cl) match {
        case Some(m) => {
          // Need to shell out to arv-put to upload scatterdir
          val commandLine = Array("arv-put", "--no-progress", "--portable-data-hash", m.group(1))
          val stdoutSettings = new OutputStreamSettings
          val stderrSettings = new OutputStreamSettings

          var temp = java.io.File.createTempFile("PDH", ".tmp");

          stdoutSettings.setOutputFile(temp, true)

          val processSettings = new ProcessSettings(
            commandLine, false, function.commandDirectory, null,
            null, stdoutSettings, stderrSettings)

          var controller = ProcessController.getThreadLocal
          val exitStatus = controller.exec(processSettings).getExitValue
          if (exitStatus != 0) {
            updateStatus(RunnerStatus.FAILED)
            return
          }

          vwdpdh = Files.readAllLines(Paths.get(temp.getPath()), Charset.defaultCharset()).get(0)
          println(vwdpdh)
        }
        case None => {}
      }

      cl = rx.replaceFirstIn(cl, "'-L' '$2'")

      var parameters = new HashMap[String, Object]()
      var cmdLine = new ArrayList[String]
      cmdLine.add("/bin/sh")
      cmdLine.add("-c")
      cmdLine.add(function.commandLine)
      parameters.put("command", cmdLine)
      parameters.put("task.vwd", vwdpdh)

      body.put("script_parameters", parameters)

      var json = new JSONObject(body)
      var p = new HashMap[String, Object]()
      p.put("job", json.toString())
      var response = arv.call("jobs", "create", p)

      jobUuid = response.get("uuid").asInstanceOf[String]
      println(jobUuid)

      updateStatus(RunnerStatus.RUNNING)
    }
  }

  def updateJobStatus() = {
    arv.synchronized {
      var p = new HashMap[String, Object]()
      p.put("uuid", jobUuid)
      var response = arv.call("jobs", "get", p)

      var returnStatus: RunnerStatus.Value = null
      var state = response.get("state")
      println(state)
      state match {
        case "Queued" => returnStatus = RunnerStatus.RUNNING
        case "Running" => returnStatus = RunnerStatus.RUNNING
        case "Complete" => {
          Files.createSymbolicLink(Paths.get(outfilePath), Paths.get("/keep/" + response.get("output") + "/" + outfileName))
          returnStatus = RunnerStatus.DONE
        }
        case "Failed" => returnStatus = RunnerStatus.FAILED
        case "Cancelled" => returnStatus = RunnerStatus.FAILED
      }

      println(returnStatus)
      updateStatus(returnStatus)
      true
    }
  }

  def tryStop() {
    try {
      var p = new HashMap[String, Object]()
      p.put("uuid", jobUuid)
      p.put("job", "")
      var response = arv.call("jobs", "cancel", p)
    } catch {
      case e: com.google.api.client.http.HttpResponseException => {}
    }
  }
}
