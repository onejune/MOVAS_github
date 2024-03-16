package com.mobvista.data.trynew.dict_v1.merge

import com.mobvista.data.trynew.dict_v1.campaign_list.increment
import com.mobvista.data.trynew.dict_v1.dict_generation
import org.apache.spark.SparkContext

class DataGeneration {

  val increData = new increment.DataGeneration()
  val dictData = new dict_generation.DataGeneration()

  def generate(sc: SparkContext, dateHour: String, sqlStepLength: Int): Unit = {

    increData.generate(sc, dateHour, sqlStepLength)
    dictData.generate(sc, dateHour)

    ()
  }

}
