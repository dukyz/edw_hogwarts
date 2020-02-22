package com.se.edw.env

trait EnvEDW extends EnvBasic{

    /**
      * @TODO functional area under a domain object
      */
    val Domain_Area = EnvEDW.Domain_Area
    /**
      * @TODO basic and extra model for domain object
      */
    val Domain_Model = EnvEDW.Domain_Model
    /**
      * @TODO etl mode for domain object
      */
    val Domain_ETL = EnvEDW.Domain_ETL

}


object EnvEDW  {

    /**
      * @TODO functional area under a domain object
      */
    object Domain_Area {
        val Meta_area = "_meta"
        val Model_area = "_model"
        val Input_area = "_input"
        val Output_area = "_output"
        val Error_area = "_error"
        val Archive_area = "_archive"
        //        val Tag_area = "_tag"
        val Process_area = "_process"
    }
    /**
      * @TODO basic and extra model for domain object
      */
    object Domain_Model {
        val Delta = "delta"
        val Snapshot = "snapshot"
        val Ssd_type2 = "chain"
    }
    /**
      * @TODO etl mode for domain object
      */
    object Domain_ETL {
        val Append = "append"
        val Merge = "merge"
        val Overwrite = "overwrite"
    }

}