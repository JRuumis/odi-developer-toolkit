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
println("All mapping Datastores, both Sources and Targets: ${sourceTargetDatastoreNames}")

// get all Target Datastore names
MapPhysicalDesign physicalDesign  = existingMapping.getPhysicalDesign(0)
List targetNodes = physicalDesign.getTargetNodes()
List targetDatastoreNames = targetNodes.collect {it.getLogicalComponent().getName()}
println ("Target Datastore names only: ${targetDatastoreNames}")

// get only the Source Datastore name
def sourceDatastoreNames = sourceTargetDatastoreNames.minus(targetDatastoreNames)
def sourceDatastores = sourceTargetDatastores.findAll { sourceDatastoreNames.contains(it.getName()) }


println("Source Datastore names only: ${sourceDatastoreNames}")
println(sourceDatastores)
println(sourceDatastores.collect { [it.getName(), it.getLogicalSchemaName()] } )


def mappingFilters = existingMapping.getAllComponentsOfType('FILTER')
def mappingFilterConditions = mappingFilters.collect { it.getFilterConditionText() }
println( mappingFilters.collect { [it.getName(), it.getFilterConditionText()]  } )

def tableNamePattern = "([0-9a-zA-Z_]+)"
def cdcColumnNames = ["(GG_MODIFIED_DATE)", "(CDC_MODIFIED_DATE)"]
def cdcSearchPatterns = cdcColumnNames.collect { tableNamePattern + "\\." + it }

//def cdcSearchPatterns = [ /([0-9a-zA-z_]+)\.GG_MODIFIED_DATE/ , /([0-9a-zA-z_]+)\.CDC_MODIFIED_DATE/ ]
//def cdcSearchPatterns = [ /(GG_MODIFIED_DATE)/ , /(CDC_MODIFIED_DATE)/ ]
//println(cdcSearchPatterns)

def cdcColumns = mappingFilterConditions.collect { filterCond -> cdcSearchPatterns.collect { pattern -> 

  def matchResult = filterCond =~ pattern 
  
  if (matchResult.find()) {
    [tableName: matchResult[0][1], columnName: matchResult[0][2] ]
  } else {
    null
  }
}}.flatten().findAll {it != null}

println("CDC Columns:\n${cdcColumns}")










