package com.se.edw.model.domain

import com.se.edw.env.{EnvAWS, EnvEDW}


case class DomainObject(schemaObject: DomainObjectSchema) extends EnvEDW with EnvAWS{

    private val _archivePrefix =
        if (schemaObject.bucketLocation.equals(schemaObject.archiveBucketLocation))
            Domain_Area.Archive_area
        else
            ""

    val inputArea = schemaObject.shiftToArea(Domain_Area.Input_area)
    val modelArea = schemaObject.shiftToArea(Domain_Area.Model_area)
    val outputArea = schemaObject.shiftToArea(Domain_Area.Output_area)
    val archiveArea = _archivePrefix + schemaObject.shiftToArea(Domain_Area.Model_area)





}
