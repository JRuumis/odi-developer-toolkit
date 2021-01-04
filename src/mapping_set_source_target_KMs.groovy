


/*
Mapping existingMapping
logger.debug("Searching for an existing ODI mapping, project code: ${projectCode}, mapping name: ${mappingName}")

def existingMappings = odi.finder.mappingFinder.findByName(mappingName, projectCode)
if (existingMappings.size() != 1) {
    logger.error("Mapping not found. (Or too many mappings found.) Expected found: 1, actual found: ${existingMappings.size()}. Mapping name: ${mappingName}, Project Code: ${projectCode}")
    return [] // <------- need to exit() here
} else {
    logger.info("Mapping found: ${mappingName}")
    existingMapping = existingMappings[0]
}


def sourceTargetDatastores = existingMapping.getAllComponentsOfType('DATASTORE')
def sourceTargetDatastoreNames = sourceTargetDatastores.collect {it.getName()}
*/
/////////////////////////////////////////////////////////////////




logger.tabZero()
logger.strongSplitter()
logger.info("Starting Source-to-Staging mapping generation.")
logger.info("Setting up ODI access...")

OdiAccess odi = new OdiAccess(odiInstance)

// tst10
String oracleDbUrl = "jdbc:oracle:thin:@tst10dwh-scan.mfltest.co.uk:1560/tst10dwh.mfltest.co.uk"
String oracleDbUser = "VRMDM_CONTROL"
String oracleDbPassword = "sophie"
String oracleDbDriver = "oracle.jdbc.pool.OracleDataSource"



Sql sql = Sql.newInstance(oracleDbUrl, oracleDbUser, oracleDbPassword, oracleDbDriver)


// from DB
//String projectCode = 'VRM_SANDBOX'
//String folderName = 'AutomationScripts'

// from DB
//String ikmName = 'IKM Oracle Control Append'
//String lkmName = 'LKM SQL to Oracle'

//logger.info("ODI access parameters set: ODI project code: ${projectCode}, ODI folder name: ${folderName}, LKM name: ${lkmName}, IKM name: ${ikmName}")

logger.strongSplitter()
logger.info("Starting to iterate through mappings:")
logger.tabIncrease()
logger.splitter()


// different table. another table for KM param setting
sql.eachRow("SELECT * FROM VRMDM_CONTROL.ODI_AUTO__SET_SRC_TGT_KM") { sourceRow ->

    logger.debug("source row: ${sourceRow}")

    String projectCode      = sourceRow["PROJECT_CODE"]
    String folderName       = sourceRow["FOLDER_NAME"]
    String mappingName      = sourceRow["MAPPING_NAME"]
    String ikmName          = sourceRow["MAPPING_NAME"]
    String lkmName          = sourceRow["MAPPING_NAME"]

    /*
    String mappingName     = sourceRow["MAPPING_NAME"]
    String sourceModel     = sourceRow["SOURCE_MODEL"]
    String sourceTable     = sourceRow["SOURCE_TABLE"]
    String targetModel     = sourceRow["TARGET_MODEL"]
    String targetTable     = sourceRow["TARGET_TABLE"]
    String activeYN        = sourceRow["ACTIVE_YN"].toUpperCase() // todo: to Boolean
    String deleteIfExists  = sourceRow["DELETE_IF_EXSTS_YN"].toUpperCase()
    */

    logger.debug("Line values: $mappingName, $sourceModel, $sourceTable, $targetModel, $targetTable, $activeYN")

    //if (activeYN != 'Y') {
    //    logger.debug("Skipping mapping $mappingName because activeYN flag not set to 'Y'.")
    //} else {

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

        // Check if mapping exists
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

        //if (deleteIfExists == "Y" && mappingExists) {
        //    logger.info("Deleting existing mapping...")
        //    odi.deleteEntity(existingMapping)
        //    logger.debug("Existing mapping deleted.")
        //}

        if (/*deleteIfExists != "Y" &&*/ !mappingExists) {
            logger.error("Mapping $mappingName does not exist!")
        } else {

            // Create the Mapping itself

            /*

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

             */


/*
            // Add a Data Set
            logger.splitterDebug()

            logger.debug("Creating Dataset...")
            Dataset dataSet = (Dataset) map.createComponent("DATASET", "DEFAULTDATASET");
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
*/

            // ----- KM Setup -----
            logger.splitterDebug()
            logger.info("Setting up Physical Design and KMs...")
            //map.createPhysicalDesign('Default')

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
    //}

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






