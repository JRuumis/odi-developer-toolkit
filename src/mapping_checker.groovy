//Created by DI Studio
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.physical.MapPhysicalDesign
import oracle.odi.domain.project.OdiPackage
import oracle.odi.domain.project.StepOdiCommand
import oracle.odi.domain.adapter.project.IKnowledgeModule
import oracle.odi.domain.model.OdiModel
import oracle.odi.domain.model.OdiKey

import java.util.regex.Matcher
import java.util.regex.Pattern

import groovy.sql.Sql

String controlSchemaUrl = "jdbc:oracle:thin:@midwhdev01.A953402997598.AMAZONAWS.COM:1521/DEVDW"
String controlSchemaUser = "ODI_USER"
String controlSchemaPassword = "charlie"
String oracleDriver = "oracle.jdbc.pool.OracleDataSource"

ConsoleLogger logger = new ConsoleLogger(this, ConsoleLogger.LoggingLevel.DEBUG)
logger.tabZero()
logger.info("Setting up ODI access...")
OdiAccess odi = new OdiAccess(odiInstance)

logger.info("Setting up DB access...")
Sql sql = Sql.newInstance(controlSchemaUrl, controlSchemaUser, controlSchemaPassword, oracleDriver)

def listToString(list) {
    if(list.size() == 0) ""
    else if (list.size() == 1) list[0]
    else {
        Integer index = 0

        list.collect { li ->
            index = index + 1
            index.toString() + ") " + li
        }.join("\r")
    }
}




// check Mappings
logger.tabZero()
logger.strongSplitter()
logger.info("Starting MAPPING CHECKER.")

sql.execute("TRUNCATE TABLE MAP_CHECK_OUTPUT")
sql.eachRow("SELECT * FROM MAP_CHECK_INPUT WHERE ENABLED='Y'") { checkMapping ->

    String projectCode = checkMapping["PROJECT_CODE"]
    String folderName = checkMapping["FOLDER_NAME"]

    logger.strongSplitter()
    logger.info("Checking: ODI Project Code: ${projectCode}, ODI Folder Name: ${folderName}")
    logger.info("Starting to iterate through mappings...")
    logger.tabIncrease()
    logger.splitter()

    def mappingsFound = odi.finder.mappingFinder.findByProject(projectCode, folderName)

    mappingsFound.collect { mapping ->

        String mappingName = mapping.getName()

        logger.info("Checking mapping ${mappingName}...")
        logger.tabIncrease()

        MapPhysicalDesign physicalDesign = mapping.getPhysicalDesign(0)
        //logger.debug("Physical Design created: $physicalDesign")

         // get IKM settings
        String ikmName
        String ikmOptions

        List targetNodes = physicalDesign.getTargetNodes()
        if (targetNodes.size() < 1) {
            ikmName = ""
            ikmOptions = ""
            logger.info("No Target nodes found for this mapping.")
        } else if (targetNodes.size() == 1) {

            def targetNode = targetNodes[0]
            IKnowledgeModule nodeIkm = targetNode.getIKM()

            if (nodeIkm) {
                ikmName = nodeIkm.getName()
                ikmOptions = (targetNode.getIKMOptionValues().collect { opVal -> "${opVal.getName()}=${opVal.getOptionValueString()}" }).toString().replace("'","''")

                logger.info("IKM node: ${ikmName}")
                logger.info("IKM options: ${ikmOptions})")
            } else {
                ikmName = ""
                ikmOptions = ""

                logger.error("Target Node ${targetNode.getName()} does not have an IKM defined!")
            }
        } else {
            Integer index = 0
            def nodesOptions = targetNodes.collect { targetNode ->

                IKnowledgeModule nodeIkm = targetNode.getIKM()

                if (nodeIkm) {
                    ikmName = nodeIkm.getName()
                    ikmOptions = (targetNode.getIKMOptionValues().collect { opVal -> "${opVal.getName()}=${opVal.getOptionValueString()}" }).toString().replace("'","''")

                    index = index + 1
                    [node: ikmName, options: ikmOptions, index: index]
                } else {
                    [node: null, options: null, index: null]
                }
            }

            ikmName = nodesOptions.collect{ it["node"] == null ? null : it["index"].toString() + ") " + it["node"] }.findAll{it != null}.join("\r")
            ikmOptions = nodesOptions.collect{ it["options"] == null ? null : it["index"].toString() + ") " + it["options"] }.findAll{it != null}.join("\r")
        }

        // get LKM settings
        String lkmName
        String lkmOptions

        List apNodes = physicalDesign.getAllAPNodes()
        if (apNodes.size() < 1) {
            lkmName = ""
            lkmOptions = ""
            logger.info("No AP nodes found for this mapping.")
        } else if (apNodes.size() == 1) {

            def apNode = apNodes[0]
            IKnowledgeModule nodeLkm = apNode.getLKM()

            if (nodeLkm) {
                lkmName = nodeLkm.getName()
                lkmOptions = (apNode.getLKMOptionValues().collect { opVal -> "${opVal.getName()}=${opVal.getOptionValueString()}" }).toString().replace("'","''")

                logger.info("LKM node: ${lkmName}")
                logger.info("LKM options: ${lkmOptions})")
            } else {
                lkmName = ""
                lkmOptions = ""

                logger.error("Target Node ${apNode.getName()} does not have an LKM defined!")
            }
        } else {
            Integer index = 0
            def nodesOptions = apNodes.collect { apNode ->

                IKnowledgeModule nodeLkm = apNode.getLKM()

                if (nodeLkm) {
                    lkmName = nodeLkm.getName()
                    lkmOptions = (apNode.getLKMOptionValues().collect { opVal -> "${opVal.getName()}=${opVal.getOptionValueString()}" }).toString().replace("'","''")

                    index = index + 1
                    [node: lkmName, options: lkmOptions, index: index]

                    //logger.info("LKM node: ${lkmName}")
                    //logger.info("LKM options: ${lkmOptions})")
                } else {
                    [node: null, options: null, index: null]
                }
            }

            lkmName = nodesOptions.collect{ it["node"] == null ? null : it["index"].toString() + ") " + it["node"] }.findAll{it != null}.join("\r")
            lkmOptions = nodesOptions.collect{ it["options"] == null ? null : it["index"].toString() + ") " + it["options"] }.findAll{it != null}.join("\r")
        }


        String insertStatement =
                "INSERT INTO MAP_CHECK_OUTPUT " +
                "(PROJECT_CODE, FOLDER_NAME, CHECK_DATETIME, MAPPING_NAME, " +
                "NR_OF_TARGET_NODES, FIRST_NODE_IKM, FIRST_NODE_IKM_OPTION_VALS, " +
                "NR_OF_AP_NODES, FIRST_NODE_LKM, FIRST_NODE_LKM_OPTION_VALS) " +
                "VALUES " +
                "('${projectCode}', '${folderName}', SYSDATE, '${mappingName}', " +
                "${targetNodes.size()}, NULLIF('${ikmName}',''), NULLIF('${ikmOptions}',''), " +
                "${apNodes.size()}, NULLIF('${lkmName}',''), NULLIF('${lkmOptions}','') )"

        //logger.debug(insertStatement)
        sql.execute(insertStatement)

        logger.tabDecrease()
    }

    logger.tabDecrease()
}



// check Model
logger.tabZero()
logger.strongSplitter()
logger.info("Starting MODEL CHECKER.")

sql.execute("TRUNCATE TABLE MODEL_CHECK_OUTPUT_TABLES")
sql.execute("TRUNCATE TABLE MODEL_CHECK_OUTPUT_COLUMNS")
sql.eachRow("SELECT * FROM MODEL_CHECK_INPUT WHERE ENABLED='Y'") { checkModel ->

    String modelCode = checkModel["MODEL_CODE"]

    logger.info("Checking: ODI Model Code: ${modelCode}")

    OdiModel model = odi.finder.modelFinder.findByCode(modelCode)

    if (model == null) {
        logger.error("ODI Model with the code ${modelCode} not found! (Is Model Code different from Model Name?)")
    } else {
        logger.info("ODI Model found.")

        def dataStores = model.getDataStores()
        logger.info("${dataStores.size()} Data Stores found in Model.")
        logger.info("Starting to iterate through Data Stores...")
        logger.tabIncrease()

        Integer tableIndex = 1
        Integer columnIndex = 1
        dataStores.each { dataStore ->
            logger.info("Checking Data Store: ${dataStore.getName()}")

            logger.tabIncrease()

            def columns = dataStore.getColumns()
            //Integer rowCount = dataStore.getRowCount()
            OdiKey primaryKey = dataStore.getPrimaryKey()
            def primaryKeyColumns
            if (primaryKey) {
                primaryKeyColumns = primaryKey.getColumns().collect { it.getName() }
            } else {
                primaryKeyColumns = []
            }

            def references = dataStore.getOutboundReferences()

            //logger.info("Row Count:  ${rowCount}")
            logger.info("Primary Key: ${primaryKeyColumns.join(", ")}")
            logger.info("Found ${columns.size()} Data Store columns.")

            logger.info("References: ${references}")
            logger.tabIncrease()
            def foreignKeys = references.collect { reference ->

                def fkColumnNames = reference.getReferenceColumns().collect { refCol -> refCol.getForeignKeyColumn().getName() }

                String fkReference = reference.getReferenceColumns().collect { refCol ->
                    "${refCol.getForeignKeyColumn().getName()} = ${refCol.getPrimaryKeyColumn().getTable().getName()}.${refCol.getPrimaryKeyColumn().getName()}"
                }.join(" AND ")

                String fkReferencedDataStore = reference.getPrimaryDataStore().getName()

                logger.info("references data store: ${reference.getPrimaryDataStore().getName()}, reference: ${fkReference} ")

                fkColumnNames.collect { col -> [column: col, refTable: fkReferencedDataStore, reference: fkReference] }
            }.flatten()
            logger.tabDecrease()

            logger.info("Found ${foreignKeys.size()} FKs.")

            def tableNamingCompliance = dataStore.getName() ==~ /(?:(META_)([A-Z0-9_]+))|([A-Z](?:[A-Z0-9_])*?)(VRM)(?:(_F_HIST)|(_F_SNAP[A-Z]+)|(_F[A-Z]+AGG)|(_D_SCD)|(_D_STG[1-9][0-9]*0)|(D_TMP[1-9][0-9]*0)|(_D_INT)|(_F)|(_D))/
            logger.info("Table naming compliance: ${tableNamingCompliance.toString()}")

            logger.info("Columns:")
            logger.tabIncrease()
            columns.each { column ->

                String columnName = column.getName()
                String columnDataType = column.getDdlDataType()
                String columnIsMandatory = column.isMandatory()

                def columnForeignKeys = listToString(foreignKeys.findAll{ it["column"] == columnName }.collect{ "Ref Table: ${it["refTable"]}, Reference: ${it["reference"]}" })
                Boolean isPrimaryKey = (primaryKeyColumns.find{ it == columnName }) ? true : false

                logger.info("name: ${columnName}: data type: ${columnDataType}, is mandatory: ${columnIsMandatory}, is PK: ${isPrimaryKey}, column FKs: ${columnForeignKeys}")

                String sqlStatement = "INSERT INTO MODEL_CHECK_OUTPUT_COLUMNS " +
                        "(COLUMN_ID, TABLE_ID, COLUMN_NAME, DATA_TYPE, IS_MANDATORY, IS_PK, FOREIGN_KEYS) " +
                        "VALUES " +
                        "(${columnIndex}, ${tableIndex}, '${columnName}', '${columnDataType}', '${columnIsMandatory ? 'Y' : 'N'}', '${isPrimaryKey ? 'Y' : 'N'}', '${columnForeignKeys}')"

                //logger.debug(sqlStatement)
                sql.execute(sqlStatement)

                columnIndex = columnIndex + 1
            }
            logger.tabDecrease()
            logger.tabDecrease()

            String sqlStatement = "INSERT INTO MODEL_CHECK_OUTPUT_TABLES " +
                    "(TABLE_ID, MODEL_CODE, DATA_STORE_NAME, TABLE_NAMING_CONVENTION_COMPL, PRIMARY_KEY_COLUMNS, NR_OF_COLUMNS, NR_OF_FOREIGN_KEYS) " +
                    "VALUES " +
                    "(${tableIndex}, '${modelCode}', '${dataStore.getName()}', '${tableNamingCompliance ? 'Y' : 'N'}', '${primaryKeyColumns.join(", ")}', ${columns.size()}, ${foreignKeys.size()})"
            sql.execute(sqlStatement)

            tableIndex = tableIndex + 1
        }



    }

}

