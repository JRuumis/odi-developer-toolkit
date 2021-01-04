
// this script is for creating simple 1:1 source to target mappings.
// It assumes that source columns can be mapped to target columns based on simple rules.

// DONE: clean up messages
// DONE: generate scenario
// todo: refactor
// DONE: add more tabs to debug messages
// DONE: option to delete mapping before re-generation
// todo: add KM names and options to the file? Hardcoded?
// DONE: split into modules
// todo: create module SHARED and push functions like KM generation to that
// todo: create run guide MD
// DONE: config from schema. (schema not available yet)
// todo: column matching - to config file/schema
// todo: set up spreadsheet
// DONE: add message that mapping has been auto-generated
// todo: give option to add additional comment to mapping in file/schema/spreadsheet
// todo: try/catch blocks

import groovy.sql.Sql

// ODI imports
import oracle.odi.domain.adapter.project.IKnowledgeModule.ProcessingType
import oracle.odi.domain.model.OdiDataStore
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.component.DatastoreComponent
import oracle.odi.domain.mapping.physical.MapPhysicalDesign

// ---------------------------------------------------------------------------------------------------------------------

ConsoleLogger logger = new ConsoleLogger(this, ConsoleLogger.LoggingLevel.DEBUG)

enum ColumnMatchTypes {EQUALS, SRCENDSWITH, TGTENDSWITH, SRCSTARTSWITH, TGTSTARTSWITH}
enum ColumnMatchCaseTypes {MATCH, IGNORECASE}

// todo: match by position as well?
def createExpressionsByName(targetComponent, componentConnectionPoint, columnMatchType, columnMatchCaseType) {
    def sourceAttributes = null
    if (componentConnectionPoint != null)
        sourceAttributes = componentConnectionPoint.getUpstreamInScopeAttributes()
    else
        sourceAttributes = targetComponent.getUpstreamLeafAttributes(targetComponent)

    def targetAttributes = targetComponent.getAttributes()
    for (MapAttribute targetAttr : targetAttributes) {
        def targetAttrName = targetAttr.getName()
        if (columnMatchCaseType == ColumnMatchCaseTypes.IGNORECASE) targetAttrName = targetAttrName.toLowerCase()

        def sourceAttribute = null
        for (MapAttribute sourceAttr : sourceAttributes) {
            def sourceAttrName = sourceAttr.getName()
            if (columnMatchCaseType == ColumnMatchCaseTypes.IGNORECASE) sourceAttrName = sourceAttrName.toLowerCase()

            if (
                (columnMatchType == ColumnMatchTypes.SRCENDSWITH && sourceAttrName.endsWith( targetAttrName )) ||
                (columnMatchType == ColumnMatchTypes.SRCSTARTSWITH && sourceAttrName.startsWith( targetAttrName )) ||
                (columnMatchType == ColumnMatchTypes.TGTSTARTSWITH && targetAttrName.startsWith( sourceAttrName )) ||
                (columnMatchType == ColumnMatchTypes.TGTENDSWITH && targetAttrName.endsWith( sourceAttrName )) ||
                (columnMatchType == ColumnMatchTypes.EQUALS && targetAttrName.equals( sourceAttrName ))
            ) {
                sourceAttribute = sourceAttr
                break
            }
        }

        if (sourceAttribute != null && componentConnectionPoint != null)
            targetAttr.setExpression( componentConnectionPoint, sourceAttribute, null )
        else if (sourceAttribute != null)
            targetAttr.setExpression( sourceAttribute )
    }
}


// ---------------------------------------------------------------------------------------------------------------------


logger.tabZero()
logger.strongSplitter()
logger.info("Starting Source-to-Staging mapping generation.")
logger.info("Setting up ODI access...")

OdiAccess odi = new OdiAccess(odiInstance)


String oracleDbUrl = "jdbc:oracle:thin:@midwhdev01.A953402997598.AMAZONAWS.COM:1521/DEVDW"
String oracleDbUser = "ODI_USER"
String oracleDbPassword = "charlie"
String oracleDbDriver = "oracle.jdbc.pool.OracleDataSource"

Sql sql = Sql.newInstance(oracleDbUrl, oracleDbUser, oracleDbPassword, oracleDbDriver)

//String sourceFilePath = "..\\resources\\interfaces.txt"
//String sourceFilePath = "C:\\Developer\\Projects\\odi-automation\\resources\\janis.txt"

// todo: add to DB?
String projectCode = 'VRM_MI'
String folderName = 'AutomationScripts'

//String ikmName = 'IKM SQL Control Append'
String ikmName = 'IKM Oracle Control Append'
String lkmName = 'LKM SQL to Oracle'

logger.info("ODI access parameters set: ODI project code: ${projectCode}, ODI folder name: ${folderName}, LKM name: ${lkmName}, IKM name: ${ikmName}")



// todo: this should come from the DB
// todo: question: delete existing mappings before recreating?
//def s = 0

logger.strongSplitter()
logger.info("Starting to iterate through mappings:")
logger.tabIncrease()
logger.splitter()

//interfacesSourceFile.eachLine { line ->

sql.eachRow("SELECT * FROM VRMDM_CONTROL.ODI_AUTO__SRC2STG WHERE ACTIVE_YN='Y'") { sourceRow ->


    logger.debug("source row: ${sourceRow}")

    //s++
    //toks = line.split(",")
 
    //if (toks.length != 7) {
    //    logger.error("Error in input, line: $s. Line appears to be of invalid format.")
    //} else {
        //String mappingName     = toks[0]
        //String sourceModel     = toks[1]
        //String sourceTable     = toks[2]
        //String targetModel     = toks[3]
        //String targetTable     = toks[4]
        //String activeYN        = toks[5].toUpperCase() // todo: to Boolean
        //String deleteIfExists  = toks[6].toUpperCase()

        String mappingName     = sourceRow["MAPPING_NAME"]
        String sourceModel     = sourceRow["SOURCE_MODEL"]
        String sourceTable     = sourceRow["SOURCE_TABLE"]
        String targetModel     = sourceRow["TARGET_MODEL"]
        String targetTable     = sourceRow["TARGET_TABLE"]
        String activeYN        = sourceRow["ACTIVE_YN"].toUpperCase() // todo: to Boolean
        String deleteIfExists  = sourceRow["DELETE_IF_EXSTS_YN"].toUpperCase()


        logger.debug("Line values: $mappingName, $sourceModel, $sourceTable, $targetModel, $targetTable, $activeYN")
        
        if (activeYN != 'Y') {
          logger.debug("Skipping mapping $mappingName because activeYN flag not set to 'Y'.")
        } else {
         
            logger.info("Processing mapping $mappingName...")
            logger.tabIncrease()

            // Find the ODI folder
            logger.splitterDebug()
            logger.debug("Searching for folder...")
            def foldersFound = odi.finder.folderFinder.findByName(folderName, projectCode)
            def odiFolder
            if (foldersFound.size() == 1) {
                odiFolder = foldersFound.iterator().next()
                logger.debug("Folder found: $odiFolder")
            } else {
                odiFolder = null
                logger.error("Folder not found. Search terms: folder name [$folderName], project code [$projectCode]")
            }

            // Check if mapping exists, delete if necessary
            logger.splitterDebug()
            logger.debug("Checking if mapping already exists...")
            Mapping existingMapping = odi.finder.mappingFinder.findByName(odiFolder, mappingName)
            Boolean mappingExists
            if (existingMapping != null) {
                mappingExists = true
                logger.info("Mapping with the name ${mappingName} in ODI folder ${odiFolder} already exists.")
            } else {
                mappingExists = false
                logger.debug("Mapping with the name ${mappingName} in ODI folder ${odiFolder} does not exist.")
            }

            if (deleteIfExists == "Y" && mappingExists) {
                logger.info("Deleting existing mapping...")
                odi.deleteEntity(existingMapping)
                logger.debug("Existing mapping deleted.")
            }

            if (deleteIfExists != "Y" && mappingExists) {
                logger.info("Skipping mapping creation because it already exists. Set the deleteIfExists flat to 'Y' if you want this mapping to be re-generated.")
            } else {

                // Create the Mapping itself
                logger.splitterDebug()
                logger.debug("Creating mapping...")
                Mapping map = new Mapping(mappingName, odiFolder)
                map.setDescription( "This mapping was auto-generated by the [${this.getClass()}] script at ${logger.currentTimestamp()}.\n" +
                                    "If the generator is run again, your changes to this mapping will be overwritten.\n" +
                                    "Exclude this mapping from the mappings list or set the deleteIfExists flag to 'N' to preserve your changes.\n\n")
                logger.debug("Mapping created: $map. Calling persist.")
                odi.persistEntity(map)
                logger.debug("Mapping saved.")
                //odiInstance.getTransactionalEntityManager().persist(map); // todo - my save function

                // Add a Data Set
                logger.splitterDebug()
                logger.debug("Creating Dataset...")
                Dataset dataSet = (Dataset) map.createComponent("DATASET", sourceTable);
                logger.debug("Dataset created: $dataSet")

                // Create Source Datastore and Component
                // todo: check if not null, show error
                logger.debug("Searching for Source Datastore, source table: $sourceTable, source model: $sourceModel")
                //OdiDataStore sourceDatastore = ((IOdiDataStoreFinder)odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(sourceTable, sourceModel)
                OdiDataStore sourceDatastore = odi.finder.dataStoreFinder.findByName(sourceTable, sourceModel)
                logger.debug("Source Datastore created: $sourceDatastore. Creating Source Datastore component...")
                DatastoreComponent sourceDatastoreComponent = (DatastoreComponent) dataSet.addSource(sourceDatastore, false)
                logger.debug("Source Datastore Component created: $sourceDatastoreComponent")

                // create Target Datastore and Component
                logger.debug("Searching for Target Datastore, target table: $targetTable, target model: $targetModel")
                //OdiDataStore targetDatastore = ((IOdiDataStoreFinder)odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(targetTable, targetModel)
                OdiDataStore targetDatastore = odi.finder.dataStoreFinder.findByName(targetTable, targetModel)
                logger.debug("Target Datastore created: $targetDatastore. Creating Target Datastore component...")
                DatastoreComponent targetDatastoreComponent  = (DatastoreComponent) map.createComponent("DATASTORE",targetDatastore, false)
                logger.debug("Target Datastore Component created: $targetDatastoreComponent")

                // Connect Data Set to Target
                logger.debug("Connect Source Dataset to Target Datastore component...")
                dataSet.connectTo(targetDatastoreComponent)
                logger.debug("Connect done.")

                // Automap Columns
                logger.splitterDebug()
                logger.debug("Auto-mapping columns, creating column expressions...")
                createExpressionsByName(targetDatastoreComponent, null ,ColumnMatchTypes.EQUALS, ColumnMatchCaseTypes.IGNORECASE)
                logger.debug("Column expressions created.")


                // ----- KM Setup -----
                logger.splitterDebug()
                logger.info("Setting up Physical Design and KMs...")
                map.createPhysicalDesign('Default')
                MapPhysicalDesign physicalDesign  = map.getPhysicalDesign(0)
                logger.debug("Physical Design created: $physicalDesign")

                // IKM Setup
                logger.splitterDebug()
                logger.debug("Setting up IKM...")
                logger.tabIncrease()
                logger.debug("Searching for IKM '$ikmName' in Project $projectCode")
                def foundIKMs = odi.finder.ikmFinder.findByName(ikmName, projectCode)
                if (foundIKMs.size() == 1) {
                    logger.debug("IKM found. Setting up Target Node IKM...")
                    def ikm = foundIKMs[0]

                    // setting IKM
                    List targetNodes = physicalDesign.getTargetNodes()
                    targetNodes.each {targetNode ->
                        targetNode.setIKM(ikm)
                        logger.debug("IKM for node set.")

                        // Set KM Options
                        logger.debug("Setting up IKM options...")

                        targetNode.getOptionValue(ProcessingType.TARGET, "TRUNCATE").setValue("true")
                        targetNode.getOptionValue(ProcessingType.TARGET, "FLOW_CONTROL").setValue("false")
                        // add more option values here

                        logger.debug("IKM options for node set.")
                        logger.tabDecrease()
                        logger.debug("IKM set up for node.")
                    }
                } else {
                    logger.tabDecrease()
                    logger.error("IKM with the name '$ikmName' in Project $projectCode NOT FOUND.")
                }


                // LKM Setup
                logger.splitterDebug()
                logger.debug("Setting up LKM...")
                logger.tabIncrease()
                logger.debug("Searching for LKM '$lkmName' in project $projectCode")
                def foundLKMs = odi.finder.lkmFinder.findByName(lkmName, projectCode)
                if (foundLKMs.size() == 1) {
                    logger.debug("LKM found (${foundLKMs.size()}). Setting up AP node LKM...")
                    def lkm = foundLKMs[0]

                    // setting LKM
                    List apNodes = physicalDesign.getAllAPNodes()
                    apNodes.each { apNode ->

                        logger.debug("AP node: $apNode")

                        apNode.setLKM(lkm)
                        logger.debug("LKM for node set.")

                        // Set KM Options
                        logger.debug("Setting up LKM options...")

                        //apNode.getOptionValue(ProcessingType.TARGET,"TRUNCATE").setValue("true")
                        // add more option values here

                        logger.debug("LKM options for node set.")
                        logger.tabDecrease()
                        logger.debug("LKM set up for node.")
                    }

                } else {
                    logger.tabDecrease()
                    logger.error("LKM with the name '$lkmName' in Project $projectCode NOT FOUND.")
                }


                // Create Mapping Scenario and save
                logger.splitterDebug()
                logger.debug("Saving mapping...")
                odi.saveEntity(map)
                logger.debug("Mapping saved.")

                String scenarioName = odi.normaliseScenarioName(mappingName)
                logger.info("Generating mapping scenario $scenarioName...")
                odi.createAndSaveScenario(map, scenarioName)
                logger.debug("Scenario created.")


                logger.info("Mapping $mappingName processed.")
            }
            logger.tabDecrease()
        }

        logger.splitter()
    //}


}
sql.close()
logger.tabDecrease()

logger.strongSplitter()
logger.info("Saving repository changes...")
odi.commit()
logger.info("Repository changes saved.")
//odiInstance.getTransactionManager().commit(txnStatus);
//odiInstance.close();

logger.info("done")
logger.strongSplitter()

