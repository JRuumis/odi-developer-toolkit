//Created by DI Studio
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.mapping.physical.MapPhysicalDesign

import groovy.sql.Sql



def xmlExtractUrl = "jdbc:oracle:thin:@tst1dwh-scan.mfltest.co.uk:1560/tst1dwh.mfltest.co.uk"
def xmlExtractUser = "VRMDM_CONTROL"
def xmlExtractPassword = "sophie"
def xmlExtractDriver = "oracle.jdbc.pool.OracleDataSource"


ConsoleLogger logger = new ConsoleLogger(this, ConsoleLogger.LoggingLevel.DEBUG)

logger.tabZero()
logger.strongSplitter()
//logger.info("Starting Source-to-Staging mapping generation.")
logger.info("Setting up ODI access...")

OdiAccess odi = new OdiAccess(odiInstance)



def getMappingCdcColumns(String projectCode, String mappingName, odi, logger) {

  logger.info("Getting Mapping CDC Columns...")
  logger.tabIncrease()

  // == search for the Odi Mapping ===
  Mapping existingMapping
  logger.debug("Searching for an existing ODI mapping, project code: ${projectCode}, mapping name: ${mappingName}")
  def existingMappings = odi.finder.mappingFinder.findByName(mappingName, projectCode)
  if (existingMappings.size() != 1) {
    logger.error("Mapping not found. (Or too many mappings found.) Expected found: 1, actual found: ${existingMappings.size()}. Mapping name: ${mappingName}, Project Code: ${projectCode}")
    return []
  } else {
    logger.info("Mapping found: ${mappingName}")
    existingMapping = existingMappings[0]
  }


  // === get Sources and their Logical Schemas ===

  // get all Datastore names, both Sources and Targets
  logger.debug("Querying Mapping Datastores...")
  def sourceTargetDatastores = existingMapping.getAllComponentsOfType('DATASTORE')
  def sourceTargetDatastoreNames = sourceTargetDatastores.collect {it.getName()}
  logger.tabIncrease()
  logger.debug("All mapping Datastores, both Sources and Targets: ${sourceTargetDatastoreNames}")
  logger.tabDecrease()

  // get all Target Datastore names
  logger.debug("Querying Mapping Target Datastores...")
  MapPhysicalDesign physicalDesign  = existingMapping.getPhysicalDesign(0)
  List targetNodes = physicalDesign.getTargetNodes()
  List targetDatastoreNames = targetNodes.collect {it.getLogicalComponent().getName()}
  logger.tabIncrease()
  logger.debug("Target Datastore names only: ${targetDatastoreNames}")
  logger.tabDecrease()

  // get only the Source Datastore name
  def sourceDatastoreNames = sourceTargetDatastoreNames.minus(targetDatastoreNames)
  logger.debug("Source Datastore names only: ${sourceDatastoreNames}")
  def sourceDatastoreComponents = sourceTargetDatastores.findAll { sourceDatastoreNames.contains(it.getName()) }

  //logger.info("Datastores and logical schemas: ${sourceDatastoreComponents.collect { [it.getName(), it.getLogicalSchemaName()] }}")

  logger.debug("Creating Map for Source Datastore components...")
  def sourcesWithSchemas = sourceDatastoreComponents.collect {

    def logicalTableName = it.getName()
    //def datastore = it.getBoundDataStore()
    def physicalTableName = it.getBoundDataStore().getName()

    //println("------ ${it.getBoundDataStore()} ${it.getBoundDataStore().getName()}")

    def logicalSchemaName = it.getLogicalSchemaName()
    //def logicalSchema = odi.finder.logicalSchemaFinder.findByName(logicalSchemaName)

    //println(logicalSchema)
    //def physicalSchema = logicalSchema.getPhysicalSchema(???) // cannot be done because don't have context
    //println("datastore component: ${it}, datastore: ${datastore}")

    //println("physical table name: ${physicalTableName}")

    [logicalTableName: logicalTableName, physicalTableName: physicalTableName, logicalSchemaName: logicalSchemaName]
  }
  logger.tabIncrease()
  logger.debug("Sources with schemas: ${sourcesWithSchemas}")
  logger.tabDecrease()


  // === get filters and CDC tables.columns ===

  logger.debug("Getting all Mapping Filters...")
  def mappingFilters = existingMapping.getAllComponentsOfType('FILTER')
  def mappingFilterConditions = mappingFilters.collect { it.getFilterConditionText() }
  //logger.debug("Mapping filters:\n${mappingFilters.collect { [it.getName(), it.getFilterConditionText()]  }}\n" )
  logger.debug("Mapping filters found: ${mappingFilters.size()}")
  //logger.debug("Mapping filters found: ${mappingFilters.size()}\n${mappingFilterConditions}")


  logger.debug("Parsing filter conditions to extract tables and CDC columns...")
  def tableNamePattern = "([0-9a-zA-Z_]+)"
  def cdcColumnNames = ["(GG_MODIFIED_DATE)", "(CDC_MODIFIED_DATE)"]
  def cdcSearchPatterns = cdcColumnNames.collect { tableNamePattern + "\\." + it }

  def cdcColumns = mappingFilterConditions.collect { filterCond -> cdcSearchPatterns.collect { pattern ->
    def matchResult = filterCond =~ pattern

    if (matchResult.find()) {

      def matchesFound = matchResult.size()
      def cols = []

      for (i = 0; i < matchesFound; i++) {
        cols += [logicalTableName: matchResult[i][1], cdcColumnName: matchResult[i][2]]
      }

      cols

    } else {
      null
    }
  }}.flatten().findAll {it != null}

  logger.debug("CDC Columns found: ${cdcColumns}")


  // === join the two data sets ===
  logger.debug("Joining the Mapping Source and CDC datasets...")
  def mappingCdcColumns = cdcColumns.collect {cdc ->
    def foundSourceSchema = sourcesWithSchemas.find { it["logicalTableName"] == cdc["logicalTableName"] }
    if (foundSourceSchema == null) logger.error("Source schema not found!")

    [logicalTableName: cdc["logicalTableName"], physicalTableName: foundSourceSchema["physicalTableName"], logicalSchemaName: foundSourceSchema["logicalSchemaName"], cdcColumnName: cdc["cdcColumnName"]]
  }
  logger.debug("Joined dataset: ${mappingCdcColumns}")

  logger.tabDecrease()
  mappingCdcColumns
}



//logger.debug("Defining Oracle connection parameters")



//logger.debug("Creating Oracle connection")
//def sql = Sql.newInstance(xmlExtractUrl, xmlExtractUser, xmlExtractPassword, xmlExtractDriver)
//logger.debug("Oracle connection established")

//logger.debug("Querying CDC_CONTROL for Mappings to be processed...")
//sql.eachRow("SELECT * FROM CDC_CONTROL WHERE IS_ACTIVE='Y'") { controlRow ->

//  logger.tabIncrease()
//  String currentMappingName = controlRow["PROCESS_NAME"]
//  logger.debug("Current mapping name: ${currentMappingName}")

//def mappingCdcRecords = getMappingCdcColumns(controlRow["PROJECT_NAME"], controlRow["PROCESS_NAME"], odi, logger)

def mappingCdcRecords = getMappingCdcColumns("VRM_SANDBOX", "CDC_TRANS_AGREEMENT_VRM_D_INT_1", odi, logger)

println(mappingCdcRecords)

/*
  if (mappingCdcRecords.size() == 0) {
    logger.error("No Mapping CDC columns identified by the script.")
  } else {
    //logger.logger("${mappingCdcRecords.size()} CDC columns identified for the mapping ${currentMappingName}")

    logger.debug("CDC records found. Processing...")

    mappingCdcRecords.each { cdcRecord ->

      logger.debug("Processing CDC record ${cdcRecord}...")
      logger.tabIncrease()
      def metadataId = null

      logger.debug("Checking for existing CDC_METADATA record...")
      def existingMetadataRecord = sql.firstRow(
              "SELECT * FROM CDC_METADATA " +
              "WHERE PHYSICAL_TABLE='${cdcRecord["physicalTableName"]}' AND LOGICAL_SCHEMA='${cdcRecord["logicalSchemaName"]}'")

      def existingMetadataMatrixRecord
      if (existingMetadataRecord == null) {
        logger.debug("No existing CDC_METADATA record found.")

        existingMetadataMatrixRecord = null // it cannot exist, because there is no record for it to reference
        metadataId = sql.firstRow("SELECT CDC_METADATA_ID_SEQ.NEXTVAL NEXT_SEQ_VAL FROM DUAL")["NEXT_SEQ_VAL"]

        logger.debug("Creating a new CDC_MEDATATA record...")
        sql.execute(  "INSERT INTO CDC_METADATA " +
                      "(CDC_METADATA_ID, PHYSICAL_TABLE, LOGICAL_SCHEMA, PHYSICAL_SCHEMA, DRIVING_COLUMN, MAX_QUERY_STATEMENT, CREATE_DATE) " +
                      "VALUES (${metadataId}, '${cdcRecord["physicalTableName"]}', '${cdcRecord["logicalSchemaName"]}', '[PHYSICAL_SCHEMA]', " +
                      "'${cdcRecord["cdcColumnName"]}', 'SELECT MAX(${cdcRecord["cdcColumnName"]}) FROM [PHYSICAL_SCHEMA].${cdcRecord["physicalTableName"]}', SYSDATE)")

      } else {
        logger.debug("Existing CDC_METADATA record found. Querying CDC_METADATA_ID from CDC_CONTROL_METADATA_MTX...")

        existingMetadataMatrixRecord = sql.firstRow(
                "SELECT CDC_CONTROL_ID, CDC_METADATA_ID FROM CDC_CONTROL_METADATA_MTX " +
                        "WHERE CDC_CONTROL_ID=${controlRow["CDC_CONTROL_ID"]} AND CDC_METADATA_ID=${existingMetadataRecord["CDC_METADATA_ID"]}")

        metadataId = existingMetadataRecord["CDC_METADATA_ID"]
      }

      if (existingMetadataMatrixRecord == null) {
        logger.debug("No related records found in CDC_CONTROL_METADATA_MTX. Adding a new record to CDC_CONTROL_METADATA_MTX.")
        sql.execute(  "INSERT INTO CDC_CONTROL_METADATA_MTX " +
                      "(CDC_CONTROL_ID, CDC_METADATA_ID, CREATE_DATE) " +
                      "VALUES (${controlRow["CDC_CONTROL_ID"]}, ${metadataId}, SYSDATE)" )
      } else {
        logger.debug("Related record already exists in CDC_CONTROL_METADATA_MTX. Metadata record identified: ${metadataId}")
      }

      logger.tabDecrease()
    }

  }

  logger.tabDecrease()
}
*/
