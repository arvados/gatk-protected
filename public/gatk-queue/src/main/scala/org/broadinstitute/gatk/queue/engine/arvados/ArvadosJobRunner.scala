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

    var cl = function.commandLine
    var rx = """(-Djava.io.tmpdir=)(/tmp/crunch-job-task-work/compute\d+\.\d+/output/.queue/tmp)""".r
    cl = rx.replaceAllIn(cl, "$1\\$(task.tmpdir)")

    rx = """'-o' '(/tmp/crunch-job-task-work/compute\d+\.\d+/output/.queue/scatterGather/[^/]+/[^/]+/)(.*)'""".r
    cl = rx.replaceAllIn(cl, "'-o' '\\$(task.outdir)/$2'")

    rx = """'-L' '(/tmp/crunch-job-task-work/compute\d+\.\d+/output/.queue/scatterGather/[^/]+/[^/]+/)(.*)'""".r

    cl match { case rx(scatterdir, b) => {
       // Need to shell out to arv-put to upload scatterdir

    val commandLine = Array("arv-put", "--no-progress", "--portable-data-hash", scatterdir)
    val stdoutSettings = new OutputStreamSettings
    val stderrSettings = new OutputStreamSettings

    var temp = java.io.File.createTempFile("PDH", ".tmp");

    stdoutSettings.setOutputFile(temp, true)

    val processSettings = new ProcessSettings(
      commandLine, false, function.commandDirectory, null,
      null, stdoutSettings, stderrSettings)

    var controller = ProcessController.getThreadLocal
    val exitStatus = controller.exec(processSettings).getExitValue

    var pdh = Files.readAllLines(Paths.get(temp.getPath()), Charset.defaultCharset()).get(0)
    println(pdh)

      }
    }

    cl = rx.replaceAllIn(cl, "'-L' '/keep/" + "abc+123" + "/$2'")

    var parameters = new HashMap[String, Object]()
    var cmdLine = new ArrayList[String]
    cmdLine.add("/bin/sh")
    cmdLine.add("-c")
    cmdLine.add(function.commandLine)
    parameters.put("command", cmdLine)
    //parameters.put("task.stdout", "output")

    body.put("script_parameters", parameters)

    var json = new JSONObject(body)
    var p = new HashMap[String, Object]()
    p.put("job", json.toString())
    var response = arv.call("jobs", "create", p)

    jobUuid = response.get("uuid").asInstanceOf[String]
    println(jobUuid)

    updateStatus(RunnerStatus.RUNNING)
    }

  /*
      // Set the current working directory
      drmaaJob.setWorkingDirectory(function.commandDirectory.getPath)

      // Set the output file for stdout
      drmaaJob.setOutputPath(":" + function.jobOutputFile.getPath)

      // If the error file is set specify the separate output for stderr
      // Otherwise join with stdout
      if (function.jobErrorFile != null) {
        drmaaJob.setErrorPath(":" + function.jobErrorFile.getPath)
      } else {
        drmaaJob.setJoinFiles(true)
      }

      if(!function.wallTime.isEmpty)
    	  drmaaJob.setHardWallclockTimeLimit(function.wallTime.get)

      drmaaJob.setNativeSpecification(functionNativeSpec)

      // Instead of running the function.commandLine, run "sh <jobScript>"
      drmaaJob.setRemoteCommand("sh")
      drmaaJob.setArgs(Collections.singletonList(jobScript.toString))

      // Allow advanced users to update the request via QFunction.updateJobRun()
      updateJobRun(drmaaJob)

      updateStatus(RunnerStatus.RUNNING)

      // Start the job and store the id so it can be killed in tryStop
      try {
        Retry.attempt(() => {
          try {
            jobId = session.runJob(drmaaJob)
          } catch {
            case de: DrmaaException => throw new QException("Unable to submit job: " + de.getLocalizedMessage)
          }
        }, 1, 5, 10)
      } finally {
        // Prevent memory leaks
        session.deleteJobTemplate(drmaaJob)
      }
      logger.info("Submitted job id: " + jobId)
      */
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
              //Files.createSymbolicLink(Paths.get(function.jobOutputFile.getPath),
              //  Paths.get("/keep/" + response.get("output") + "/output"))

              returnStatus = RunnerStatus.DONE
            }
            case "Failed" => returnStatus = RunnerStatus.FAILED
            case "Cancelled" => returnStatus = RunnerStatus.FAILED
      }

      println(returnStatus)
      updateStatus(returnStatus)
      true
    }
  /*
    session.synchronized {
      var returnStatus: RunnerStatus.Value = null

      try {
        val jobStatus = session.getJobProgramStatus(jobId);
        jobStatus match {
          case Session.QUEUED_ACTIVE => returnStatus = RunnerStatus.RUNNING
          case Session.DONE =>
            val jobInfo: JobInfo = session.wait(jobId, Session.TIMEOUT_NO_WAIT)
            // Update jobInfo
            def convertDRMAATime(key: String): Date = {
              val v = jobInfo.getResourceUsage.get(key)
              if ( v != null ) new Date(v.toString.toDouble.toLong * 1000) else null;
            }
            if ( jobInfo.getResourceUsage != null ) {
              getRunInfo.startTime = convertDRMAATime("start_time")
              getRunInfo.doneTime = convertDRMAATime("end_time")
              getRunInfo.exechosts = "unknown"
            }

            if ((jobInfo.hasExited && jobInfo.getExitStatus != 0)
                || jobInfo.hasSignaled
                || jobInfo.wasAborted)
              returnStatus = RunnerStatus.FAILED
            else
              returnStatus = RunnerStatus.DONE
          case Session.FAILED => returnStatus = RunnerStatus.FAILED
          case Session.UNDETERMINED => logger.warn("Unable to determine status of job id " + jobId)
          case _ => returnStatus = RunnerStatus.RUNNING
        }
      } catch {
        // getJobProgramStatus will throw an exception once wait has run, as the
        // job will be reaped.  If the status is currently DONE or FAILED, return
        // the status.
        case de: DrmaaException =>
          if (lastStatus == RunnerStatus.DONE || lastStatus == RunnerStatus.FAILED)
            returnStatus = lastStatus
          else
            logger.warn("Unable to determine status of job id " + jobId, de)
      }

      if (returnStatus != null) {
        updateStatus(returnStatus)
        true
      } else {
        false
      }
    }
    */
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
