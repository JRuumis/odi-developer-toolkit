//Created by DI Studio
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.physical.MapPhysicalDesign
import oracle.odi.domain.project.OdiPackage
import oracle.odi.domain.project.StepOdiCommand

import groovy.sql.Sql

/*
String controlSchemaUrl = "jdbc:oracle:thin:@midwhdev01.A953402997598.AMAZONAWS.COM:1521/DEVDW"
String controlSchemaUser = "VRMDM_CONTROL"
String controlSchemaPassword = "sophie"
String oracleDriver = "oracle.jdbc.pool.OracleDataSource"
*/

String controlSchemaUrl = "jdbc:oracle:thin:@10.64.21.192:1521/DEVDW"
String controlSchemaUser = "DEVDM_CONTROL"
String controlSchemaPassword = "Wh1stler2!"
String oracleDriver = "oracle.jdbc.pool.OracleDataSource"

String controlPackageFolderName = "End2End"


ConsoleLogger logger = new ConsoleLogger(this, ConsoleLogger.LoggingLevel.DEBUG)

logger.tabZero()
logger.strongSplitter()
logger.info("Starting CDC Metadata Generator...")
logger.info("Setting up ODI access...")
OdiAccess odi = new OdiAccess(odiInstance)


// helper functions
def getMappingCdcColumns(String projectCode, String mappingName, odi, logger) {

  logger.info("Getting CDC Columns for Mapping ${mappingName}...")
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
  
  // =============================================
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

  // get only the Source Datastore names
  def sourceDatastoreNames = sourceTargetDatastoreNames.minus(targetDatastoreNames)
  logger.debug("Source Datastore names only: ${sourceDatastoreNames}")

  // get Source Datastore attributes
  def sourceDatastoreComponents = sourceTargetDatastores.findAll { sourceDatastoreNames.contains(it.getName()) }
  //logger.info("Datastores and logical schemas: ${sourceDatastoreComponents.collect { [it.getName(), it.getLogicalSchemaName()] }}")

  logger.debug("Creating Map (a.k.a Dictionary) for Source Datastore components...")
  def sourcesWithSchemas = sourceDatastoreComponents.collect {
    def logicalTableName = it.getName()
    def physicalTableName = it.getBoundDataStore().getName()
    def logicalSchemaName = it.getLogicalSchemaName()

    [logicalTableName: logicalTableName, physicalTableName: physicalTableName, logicalSchemaName: logicalSchemaName]
  }
  logger.tabIncrease()
  logger.debug("Sources with schemas: ${sourcesWithSchemas}")
  logger.tabDecrease()


  // ==========================================
  // === get filters and CDC tables.columns ===
  logger.debug("Getting all Mapping Filters...")
  def mappingFilters = existingMapping.getAllComponentsOfType('FILTER')
  def mappingFilterConditions = mappingFilters.collect { it.getFilterConditionText() }
  logger.debug("Mapping Filters found: ${mappingFilters.size()}. (Each mapping can have 0 or many CDC date columns.)")

  logger.debug("Parsing Filter conditions to extract tables and CDC columns...")
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
  }}.flatten().unique().findAll {it != null}

  // adjust if Expressions referenced
  def mappingExpressionComponents = existingMapping.getAllComponentsOfType('EXPRESSION')
  //logger.debug( (mappingExpressionComponents.collect{ it.getFullName() }).toString() )

  def allMappingExps = mappingExpressionComponents.collect {mapExpComp ->
    def exps = mapExpComp.getAttributeExpressions().collect { [expCompName: mapExpComp.getName(), name: it.getExpressionOwner().getName(), exp: it.getText()] }
  }.flatten()

  //logger.debug("All mapping expressions: ${allMappingExps.toString()}")

  def cdcMappingExps = allMappingExps.collect { mappingExp -> cdcSearchPatterns.collect { pattern ->
    def matchResult = mappingExp["exp"] =~ pattern

    if (matchResult.find()) {

      def matchesFound = matchResult.size()

      def cols = []
      for (i = 0; i < matchesFound; i++) {
        cols += [logicalTableName: matchResult[i][1], cdcColumnName: matchResult[i][2]]
      }

      //mappingExp + cols[0]
      cols.collect { mappingExp + it }
    } else {
      null
    }
  }}.flatten().unique().findAll {it != null}

  logger.debug("CDC mapping expressions: ${cdcMappingExps.toString()}")


  logger.debug("CDC Columns found: ${cdcColumns}")


  def adjustedCdcColumns = cdcColumns.collect {cdcCol ->

    def adjustmentFound = cdcMappingExps.find { it["expCompName"] == cdcCol["logicalTableName"] && it["cdcColumnName"] == cdcCol["cdcColumnName"] }

    if (adjustmentFound == null) {
      cdcCol
    }
    else {
      [logicalTableName: adjustmentFound["logicalTableName"], cdcColumnName: cdcCol["cdcColumnName"]]
    }
  }

  logger.debug("Adjusted CDC Columns: ${adjustedCdcColumns}")



  // === join the two data sets ===
  logger.debug("Joining the Mapping Source and CDC datasets...")
  //def mappingCdcColumns = cdcColumns.collect {cdc ->
  def mappingCdcColumns = adjustedCdcColumns.collect {cdc ->
    def foundSourceSchema = sourcesWithSchemas.find { it["logicalTableName"] == cdc["logicalTableName"] }
    if (foundSourceSchema == null) logger.error("Source schema not found!")

    [logicalTableName: cdc["logicalTableName"], physicalTableName: foundSourceSchema["physicalTableName"], logicalSchemaName: foundSourceSchema["logicalSchemaName"], cdcColumnName: cdc["cdcColumnName"]]
  }
  logger.debug("Joined dataset: ${mappingCdcColumns}")

  logger.tabDecrease()
  mappingCdcColumns
}


def getScenarioNameFromExpression(String expression) {
    //String scenarioPattern = '"-SCEN_NAME=([A-Z0-9_]+)"'
    String scenarioPattern = '-SCEN_NAME=([A-Z0-9_]+)'
    def matchResultSc = expression =~ scenarioPattern

    if ( matchResultSc.find() )   matchResultSc[0][1]
    else                          null
}


///////////////////////////////////////////////////////////

logger.debug("Defining Oracle connection parameters")
logger.debug("Creating Oracle connection")
def sql = Sql.newInstance(controlSchemaUrl, controlSchemaUser, controlSchemaPassword, oracleDriver)
logger.debug("Oracle connection established")

logger.debug("Querying PROCESS_CONTROL for Packages to be processed...")
sql.eachRow("SELECT * FROM PROCESS_CONTROL WHERE IS_ACTIVE='Y'") { controlRow ->

  logger.tabIncrease()
  String currentPackageName = controlRow["PROCESS_NAME"]
  String packageProjectCode = controlRow["PROJECT_NAME"]

  logger.debug("Current Package name: ${currentPackageName}")
  def foundPackages = odi.finder.packageFinder.findByName(currentPackageName, packageProjectCode, controlPackageFolderName)

  OdiPackage foundPackage
  if (foundPackages.size() == 0) {
    logger.error("No packages found: currentPackageName: ${currentPackageName}, packageProjectCode: ${packageProjectCode}, controlPackageFolderName: ${controlPackageFolderName}")
  } else {
    foundPackage = foundPackages[0]
    logger.debug("Package with the name '${currentPackageName}' found.")

    def allPackageSteps = foundPackage.getSteps()
    def packageCommandSteps = allPackageSteps.findAll { it instanceof StepOdiCommand }
    def packageStepCommandNames = packageCommandSteps.collect { it.getName() }
    def packageStepCommandNamesExpressions = packageCommandSteps.collect { it.getCommandExpression() }

    //logger.debug("Expressions: ${packageStepCommandNamesExpressions}")

    def packageScenarioNamesWithNulls = packageStepCommandNamesExpressions.collect { getScenarioNameFromExpression(it.getAsString()) } // assumption: Scen names are same as mapping names
    def packageScenarioNames = packageScenarioNamesWithNulls.findAll { it != null }

            logger.debug("The following Scenario names found in the package: ${packageScenarioNames}. Scenario names assumed to match with Mapping names.")

    //println("Package: ${foundPackage},\n${foundPackage.getSteps()},\n${packageCommandSteps},\n${packageStepCommandNames},\n${packageStepCommandNamesExpressions},\n${packageScenarioNames}")

    packageScenarioNames.each { mappingName ->

      logger.debug("Processing Scenario name '${mappingName}'")
      logger.tabIncrease()

      def mappingCdcRecords = getMappingCdcColumns(packageProjectCode, mappingName, odi, logger)

      if (mappingCdcRecords.size() == 0) {
        logger.error("No Mapping CDC columns identified by the script.")
      } else {

        // insert mapping into CDC_CONTROL if does not exist
        def existingControlRecord = sql.firstRow("SELECT * FROM CDC_CONTROL WHERE " +
                "PROCESS_CONTROL_ID='${controlRow["PROCESS_ID"]}' AND " +
                "PROCESS_NAME='${mappingName}'")

        def cdcControlId = -1
        if (existingControlRecord != null) {
          cdcControlId = existingControlRecord["CDC_CONTROL_ID"]
          logger.debug("Existing CDC_CONTROL record id: ${cdcControlId}")
        } else {
          logger.debug("Creating a new CDC_CONTROL record for the mapping...")
          cdcControlId = sql.firstRow("SELECT CDC_CONTROL_ID_SEQ.NEXTVAL NEXT_SEQ_VAL FROM DUAL")["NEXT_SEQ_VAL"]
          logger.debug("New CDC_CONTROL record id: ${cdcControlId}")

          sql.execute(  "INSERT INTO CDC_CONTROL " +
                  "(CDC_CONTROL_ID, PROCESS_CONTROL_ID, PROCESS_NAME, IS_ACTIVE) " +
                  "VALUES (${cdcControlId}, '${controlRow["PROCESS_ID"]}', '${mappingName}', 'Y')")
        }


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

            //logger.debug( "Query: SELECT CDC_CONTROL_ID, CDC_METADATA_ID FROM CDC_CONTROL_METADATA_MTX " +
            //        "WHERE CDC_CONTROL_ID=${controlRow[/*"CDC_CONTROL_ID"*/"PROCESS_ID"]} AND CDC_METADATA_ID=${existingMetadataRecord["CDC_METADATA_ID"]}")

            existingMetadataMatrixRecord = sql.firstRow(
                    "SELECT CDC_CONTROL_ID, CDC_METADATA_ID " +
                    "FROM CDC_CONTROL_METADATA_MTX " +
                    "WHERE CDC_CONTROL_ID=${cdcControlId} AND CDC_METADATA_ID=${existingMetadataRecord["CDC_METADATA_ID"]}")
                    //"WHERE CDC_CONTROL_ID=${controlRow[/*"CDC_CONTROL_ID"*/"PROCESS_ID"]} AND CDC_METADATA_ID=${existingMetadataRecord["CDC_METADATA_ID"]}")

            metadataId = existingMetadataRecord["CDC_METADATA_ID"]
          }

          if (existingMetadataMatrixRecord == null) {
            logger.debug("No related records found in CDC_CONTROL_METADATA_MTX. Adding a new record to CDC_CONTROL_METADATA_MTX.")
            //logger.debug("Query: INSERT INTO CDC_CONTROL_METADATA_MTX " +
            //        "(CDC_CONTROL_ID, CDC_METADATA_ID, CREATE_DATE) " +
            //        "VALUES (${controlRow[/*"CDC_CONTROL_ID"*/"PROCESS_ID"]}, ${metadataId}, SYSDATE)")

            sql.execute(  "INSERT INTO CDC_CONTROL_METADATA_MTX " +
                    "(CDC_CONTROL_ID, CDC_METADATA_ID, CREATE_DATE) " +
                    "VALUES (${cdcControlId}, ${metadataId}, SYSDATE)" )
            //        "VALUES (${controlRow[/*"CDC_CONTROL_ID"*/"PROCESS_ID"]}, ${metadataId}, SYSDATE)" )

          } else {
            logger.debug("Related record already exists in CDC_CONTROL_METADATA_MTX. Metadata record identified: ${metadataId}")
          }

          logger.tabDecrease()
        }

      }
      logger.tabDecrease()
    }

  }

  logger.tabDecrease()
}

