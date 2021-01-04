//Created by DI Studio
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.mapping.physical.MapPhysicalDesign

import groovy.sql.Sql


//String projectCode = 'VRM_SANDBOX'
//String mappingName = 'CDC_TRANS_AGREEMENT_VRM_D_INT_1'



ConsoleLogger logger = new ConsoleLogger(this, ConsoleLogger.LoggingLevel.DEBUG)

logger.tabZero()
logger.strongSplitter()
logger.info("Starting Source-to-Staging mapping generation.")
logger.info("Setting up ODI access...")

OdiAccess odi = new OdiAccess(odiInstance)




def getMappingCdcColumns(String projectCode, String mappingName, odi, logger) {

  Mapping existingMapping
  def existingMappings = odi.finder.mappingFinder.findByName(mappingName, projectCode)
  if (existingMappings.size() != 1) {
    logger.error("Mapping not found. (Or too many mappings found.) Expected found: 1, actual found: ${existingMappings.size()}. Mapping name: ${mappingName}, Project Code: ${projectCode}")
    System.exit(1)
  } else {
    logger.info("Mapping found: ${mappingName}")
    existingMapping = existingMappings[0]
  }
  
  
  // === get Sources and their Logical Schemas ===

  // get all Datastore names, both Sources and Targets
  def sourceTargetDatastores = existingMapping.getAllComponentsOfType('DATASTORE')
  def sourceTargetDatastoreNames = sourceTargetDatastores.collect {it.getName()}
  logger.info("All mapping Datastores, both Sources and Targets: ${sourceTargetDatastoreNames}")
  
  // get all Target Datastore names
  MapPhysicalDesign physicalDesign  = existingMapping.getPhysicalDesign(0)
  List targetNodes = physicalDesign.getTargetNodes()
  List targetDatastoreNames = targetNodes.collect {it.getLogicalComponent().getName()}
  logger.info("Target Datastore names only: ${targetDatastoreNames}")
  
  // get only the Source Datastore name
  def sourceDatastoreNames = sourceTargetDatastoreNames.minus(targetDatastoreNames)
  def sourceDatastores = sourceTargetDatastores.findAll { sourceDatastoreNames.contains(it.getName()) }
  
  //logger.info("Source Datastore names only: ${sourceDatastoreNames}")
  //logger.info("Datastores and logical schemas: ${sourceDatastores.collect { [it.getName(), it.getLogicalSchemaName()] }}")
  
  def sourcesWithSchemas = sourceDatastores.collect { [tableName: it.getName(), logicalSchemaName: it.getLogicalSchemaName()] }
  

  // === get filters and CDC tables.columns

  def mappingFilters = existingMapping.getAllComponentsOfType('FILTER')
  def mappingFilterConditions = mappingFilters.collect { it.getFilterConditionText() }
  logger.info("Mapping filters:\n${mappingFilters.collect { [it.getName(), it.getFilterConditionText()]  }}\n" )
  
  def tableNamePattern = "([0-9a-zA-Z_]+)"
  def cdcColumnNames = ["(GG_MODIFIED_DATE)", "(CDC_MODIFIED_DATE)"]
  def cdcSearchPatterns = cdcColumnNames.collect { tableNamePattern + "\\." + it }
  
  def cdcColumns = mappingFilterConditions.collect { filterCond -> cdcSearchPatterns.collect { pattern -> 
    def matchResult = filterCond =~ pattern 
    
    if (matchResult.find()) {
      [tableName: matchResult[0][1], cdcColumnName: matchResult[0][2] ]
    } else {
      null
    }
  }}.flatten().findAll {it != null}
  
  logger.info("CDC Columns:\n${cdcColumns}")


  // join the two data sets

  def mappingCdcColumns = cdcColumns.collect {cdc ->

    def foundSourceSchema = sourcesWithSchemas.find { it["tableName"] == cdc["tableName"] }

    if (foundSourceSchema == null) {
      logger.error("Source schema not found!")
    }

    [sourceTable: cdc["tableName"], logicalSchemaName: foundSourceSchema["logicalSchemaName"], cdcColumnName: cdc["cdcColumnName"]]
  }

  mappingCdcColumns
}


logger.debug("Defining Oracle connection parameters")

def xmlExtractUrl = "jdbc:oracle:thin:@tst1dwh-scan.mfltest.co.uk:1560/tst1dwh.mfltest.co.uk"
def xmlExtractUser = "VRMDM_CONTROL"
def xmlExtractPassword = "sophie"
def xmlExtractDriver = "oracle.jdbc.pool.OracleDataSource"

logger.debug("Creating Oracle connection")
def schemaSql = Sql.newInstance(xmlExtractUrl, xmlExtractUser, xmlExtractPassword, xmlExtractDriver)
logger.debug("Oracle connection established")


schemaSql.eachRow("SELECT * FROM CDC_CONTROL WHERE IS_ACTIVE='Y'") { controlRow ->

  String currentMappingName = controlRow["PROCESS_NAME"]

  def mappingCdcColumns = getMappingCdcColumns(controlRow["PROJECT_NAME"], controlRow["PROCESS_NAME"], odi, logger)
  //println(mappingCdcColumns)

  if (mappingCdcColumns.size() == 0) {
    logger.error("No Mapping CDC columns identified by the script")
  } else {
    logger.info("${mappingCdcColumns.size()} CDC columns identified for the mapping ${currentMappingName}")

    mappingCdcColumns.each { cdcRecord ->

      def metadataId = null

      def existingMetadataRecord = schemaSql.firstRow(
              "SELECT * FROM CDC_METADATA " +
              "WHERE TABLE_NAME='${cdcRecord["sourceTable"]}' AND SCHEMA_NAME='${cdcRecord["logicalSchemaName"]}'")

      def existingMetadataMatrixRecord
      if (existingMetadataRecord == null) {
        existingMetadataMatrixRecord = null // it cannot exist, because there is no record for it to reference

        metadataId = schemaSql.firstRow("SELECT CDC_METADATA_ID_SEQ.NEXTVAL NEXT_SEQ_VAL FROM DUAL")["NEXT_SEQ_VAL"]

        schemaSql.execute("INSERT INTO CDC_METADATA " +
                "(CDC_METADATA_ID, TABLE_NAME, SCHEMA_NAME, DRIVING_COLUMN, CREATE_DATE) " +
                "VALUES (${metadataId}, '${cdcRecord["sourceTable"]}', '${cdcRecord["logicalSchemaName"]}', " +
                "'${cdcRecord["cdcColumnName"]}', SYSDATE)")

      } else {
        existingMetadataMatrixRecord = schemaSql.firstRow(
                "SELECT CDC_CONTROL_ID, CDC_METADATA_ID FROM CDC_CONTROL_METADATA_MTX " +
                        "WHERE CDC_CONTROL_ID=${controlRow["CDC_CONTROL_ID"]} AND CDC_METADATA_ID=${existingMetadataRecord["CDC_METADATA_ID"]}")

        metadataId = existingMetadataRecord["CDC_METADATA_ID"]
      }


      if (existingMetadataMatrixRecord == null) {
        logger.info("No related records found in CDC_CONTROL_METADATA_MTX")

        /*
        def existingMetadataRecord = schemaSql.firstRow("SELECT * FROM CDC_METADATA WHERE TABLE_NAME='${cdcRecord["sourceTable"]}' AND SCHEMA_NAME='${cdcRecord["logicalSchemaName"]}'")

        if (existingMetadataRecord == null) {
          metadataId = schemaSql.firstRow("SELECT CDC_METADATA_ID_SEQ.NEXTVAL NEXT_SEQ_VAL FROM DUAL")["NEXT_SEQ_VAL"]

          schemaSql.execute("INSERT INTO CDC_METADATA " +
                  "(CDC_METADATA_ID, TABLE_NAME, SCHEMA_NAME, DRIVING_COLUMN, CREATE_DATE) " +
                  "VALUES (${metadataId}, '${cdcRecord["sourceTable"]}', '${cdcRecord["logicalSchemaName"]}', " +
                  "'${cdcRecord["cdcColumnName"]}', SYSDATE)")
        } else {
          logger.debug("1")
          metadataId = existingMetadataMatrixRecord["CDC_METADATA_ID"]
          logger.debug("2")
        }
         */

        schemaSql.execute(  "INSERT INTO CDC_CONTROL_METADATA_MTX " +
                "(CDC_CONTROL_ID, CDC_METADATA_ID, CREATE_DATE) " +
                "VALUES (${controlRow["CDC_CONTROL_ID"]}, ${metadataId}, SYSDATE)" )

      } else {
        logger.debug("C")

       // metadataId = existingMetadataMatrixRecord["CDC_METADATA_ID"]
        logger.debug("D")

        logger.info("Metadata record identified: ${metadataId}")
      }



    }


  }




}









// TODO: next steps:
// Oracle DB connection - done
// read mappings from CDC_CONTROL - done
// check existing records in CDC_METADATA and CDC_CONTROL_METADATA_MTX - done
// insert new record in CDC_METADATA if missing - done
// insert/replace new record in CDC_CONTROL_METADATA_MTX - done

// refactor

// physical table and column name
// max_query_statement






