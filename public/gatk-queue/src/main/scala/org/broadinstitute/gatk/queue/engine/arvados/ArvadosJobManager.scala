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

import org.broadinstitute.gatk.queue.function.CommandLineFunction
import org.broadinstitute.gatk.queue.engine.CommandLineJobManager
import org.arvados.sdk.Arvados

/**
 * Runs jobs using Arvados
 */
class ArvadosJobManager extends CommandLineJobManager[ArvadosJobRunner] {
  protected var arv: Arvados = _
  protected var jobs = scala.collection.mutable.HashMap.empty[String,String]

  override def init() {
    arv = new Arvados("arvados", "v1")
  }

  def runnerType = classOf[ArvadosJobRunner]
  def create(function: CommandLineFunction) = new ArvadosJobRunner(arv, jobs, function)

  override def updateStatus(runners: Set[ArvadosJobRunner]) = {
    var updatedRunners = Set.empty[ArvadosJobRunner]
    runners.foreach(runner => if (runner.updateJobStatus()) {updatedRunners += runner})
    updatedRunners
  }

  override def tryStop(runners: Set[ArvadosJobRunner]) {
    runners.foreach(_.tryStop())
  }
}
