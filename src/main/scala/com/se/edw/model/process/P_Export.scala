package com.se.edw.model.process

import com.se.edw.env.EnvEDW._

case class P_Export(domainName:String,area:String=Domain_Area.Output_area ,
                    model:String , view:String,format:String="csv",compression:String="lzo"){

    if (area.equals(Domain_Area.Model_area))
        throw new Exception("For now you are not allow to export to _model area !!!")
}
