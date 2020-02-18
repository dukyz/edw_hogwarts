package com.se.edw.model.process

import com.se.edw.env.EnvEDW._

case class P_Import(domainName:String, area:String=Domain_Area.Model_area,
                    model:String=Domain_Model.Delta, view:String,format:String="delta"){

}
