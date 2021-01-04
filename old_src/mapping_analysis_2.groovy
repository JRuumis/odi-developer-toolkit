//Created by DI Studio
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.mapping.physical.MapPhysicalDesign


String projectCode = 'VRM_SANDBOX'
//String folderName = 'AutomationScripts'
//String mappingName = 'JanisTest'

//String folderName = 'CDCToControl'
//String mappingName = 'CDC_CONTROL_AGREEMENT_VRM_KEYS_1'

String folderName = 'CDCToTransformation'
String mappingName = 'CDC_TRANS_AGREEMENT_VRM_D_INT_1'



ConsoleLogger logger = new ConsoleLogger(this, ConsoleLogger.LoggingLevel.DEBUG)

logger.tabZero()
logger.strongSplitter()
logger.info("Starting Source-to-Staging mapping generation.")
logger.info("Setting up ODI access...")

OdiAccess odi = new OdiAccess(odiInstance)




def foldersFound = odi.finder.folderFinder.findByName(folderName, projectCode)
def odiFolder = foldersFound.iterator().next()
Mapping existingMapping = odi.finder.mappingFinder.findByName(odiFolder, mappingName)


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

def sourcesWithSchemas = 


def mappingFilters = existingMapping.getAllComponentsOfType('FILTER')
def mappingFilterConditions = mappingFilters.collect { it.getFilterConditionText() }
logger.info("Mapping filters:\n${mappingFilters.collect { [it.getName(), it.getFilterConditionText()]  }}\n" )

def tableNamePattern = "([0-9a-zA-Z_]+)"
def cdcColumnNames = ["(GG_MODIFIED_DATE)", "(CDC_MODIFIED_DATE)"]
def cdcSearchPatterns = cdcColumnNames.collect { tableNamePattern + "\\." + it }

def cdcColumns = mappingFilterConditions.collect { filterCond -> cdcSearchPatterns.collect { pattern -> 
  def matchResult = filterCond =~ pattern 
  
  if (matchResult.find()) {
    [tableName: matchResult[0][1], columnName: matchResult[0][2] ]
  } else {
    null
  }
}}.flatten().findAll {it != null}

logger.info("CDC Columns:\n${cdcColumns}")










