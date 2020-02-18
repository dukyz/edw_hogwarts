package com.se.edw
import scopt.OParser

object SchemaFileGenerator{
//java -jar schema-file-generator-assembly-0.1.jar -t ORACLE -j jdbc:oracle:thin:@10.177.145:1521:ESSB -u infa_rep -p infa -s Finance_BI_DEV -m F_ESSB_PL=ESSB_PL,F_ESSB_TOPLINE_BU=BU
//java -jar schema-file-generator-assembly-0.1.jar -l DB -t MYSQL -j jdbc:mysql://10.177.1.127:3306/MDC -u root -p 123456 -s MDC -m act_evt_log=act_evt_log1111
    def main(args: Array[String]): Unit = {

        val builder = OParser.builder[ModelLocation]
        import builder._
        val parser = OParser.sequence(
//                programName("SchemaFileGenerator"),
            head("SchemaFileGenerator", "0.1"),
            opt[String]('l', "location")
                .action((x, c) => c.copy(location = x.toUpperCase()))
                .text("where is the model ? INFA(default)/DB"),
            opt[String]('e',"etlmode")
                .action((x,c) => c.copy(etlmode = x.toUpperCase()))
                .text("new data will be handle by APPEND/MERGE/OVERWRITE(default)"),
            opt[String]('t',"dbtype")
                .required()
                .action((x,c) => c.copy(dbtype = x.toUpperCase()))
                .text("ORACLE/MYSQL"),
            opt[String]('j',"jdbc")
                .required()
                .action((x,c) => c.copy(jdbc = x))
                .text("jdbc for connection to database such as jdbc://..."),
            opt[String]('u',"username")
                .required()
                .action((x,c) => c.copy(username = x))
                .text("uesrname for database"),
            opt[String]('p',"password")
                .required()
                .action((x,c) => c.copy(password = x))
                .text("password for database"),
            opt[String]('s',"subject")
                .required()
                .action((x,c) => c.copy(subjct = x))
                .text("subjectName for INFA,schemaName for DB"),
            opt[Map[String,String]]('m',"models")
                .required()
                .valueName("modelName1=newName1,modelName2=newName2,...")
                .action((x,c) => c.copy(models = x))
                .text("names of data model"),
            checkConfig( c => {
                if (! Cons_Location.contains(c.location)){
                    failure("not supported location")
                }else if(! Cons_EtlMode.contains(c.etlmode)){
                    failure("not supported etlmode")
                }else if (! Cons_DBType.contains(c.dbtype)){
                    failure("not supported dbtype")
                }else{
                    success
                }
            })
        )

        OParser.parse(parser, args, ModelLocation()) match {
            case Some(modelLocation) => {
                modelLocation.location match {
                    case "INFA" => INFAHandler().handle(modelLocation)
                    case "DB" => modelLocation.dbtype match {
                        case "MYSQL" => MysqlHandler().handle(modelLocation)
                    }
                }
            }
            case _ =>
        }



//        val modelLocation = ModelLocation(location = "INFA",etlmode = "OVERWRITE",
//        dbtype ="ORACLE", jdbc ="jdbc:oracle:thin:@10.177.0.145:1521:ESSB",
//        username = "infa_rep",password = "infa",
//        subjct = "E2E_BI_SAP3" , models = Seq("O_MARA"))
//        INFAHandler().handle(modelLocation)


    }

}