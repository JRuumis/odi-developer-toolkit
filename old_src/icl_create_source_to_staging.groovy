import oracle.odi.core.config.MasterRepositoryDbInfo;
import oracle.odi.core.config.WorkRepositoryDbInfo;
import oracle.odi.core.config.PoolingAttributes;
import oracle.odi.core.config.OdiInstanceConfig;
import oracle.odi.core.security.Authentication;
import oracle.odi.core.persistence.transaction.ITransactionDefinition;
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition;
 
import oracle.odi.core.persistence.transaction.ITransactionManager;
import oracle.odi.core.persistence.transaction.ITransactionStatus;
 
import oracle.odi.domain.IOdiEntity
import oracle.odi.domain.topology.OdiTechnology
import oracle.odi.domain.model.OdiModel
import oracle.odi.domain.topology.OdiLogicalSchema
import oracle.odi.core.OdiInstance
import oracle.odi.domain.project.OdiVariable
 
import oracle.odi.domain.project.finder.IOdiProjectFinder
import oracle.odi.domain.topology.finder.IOdiLogicalSchemaFinder
import oracle.odi.domain.topology.finder.IOdiContextFinder
import oracle.odi.domain.topology.OdiLogicalSchema
import oracle.odi.domain.topology.OdiContext
import oracle.odi.domain.model.finder.IOdiDataStoreFinder
import oracle.odi.domain.model.finder.IOdiModelFinder
import oracle.odi.domain.model.OdiModel
import oracle.odi.domain.model.OdiColumn
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.project.finder.IOdiKMFinder
import oracle.odi.domain.project.finder.IOdiIKMFinder
import oracle.odi.domain.mapping.finder.IMappingFinder
import oracle.odi.domain.project.finder.IOdiPackageFinder
import oracle.odi.domain.project.finder.IOdiUserProcedureFinder
import oracle.odi.domain.project.finder.IOdiVariableFinder
import oracle.odi.domain.topology.finder.IOdiTechnologyFinder
import oracle.odi.domain.runtime.loadplan.finder.IOdiLoadPlanFinder
import oracle.odi.domain.runtime.scenario.finder.IOdiScenarioFinder
 
import oracle.odi.domain.project.OdiIKM
 
import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.project.OdiFolder
import oracle.odi.domain.project.OdiProject
import oracle.odi.domain.mapping.component.DatastoreComponent
import oracle.odi.domain.mapping.component.FilterComponent
import oracle.odi.domain.runtime.loadplan.OdiLoadPlan
import oracle.odi.domain.project.Step
import oracle.odi.domain.project.OdiPackage
import oracle.odi.domain.project.OdiUserProcedure
import oracle.odi.domain.project.StepVariable
 
import oracle.odi.domain.mapping.component.SetComponent
import oracle.odi.domain.mapping.component.ExpressionComponent
import oracle.odi.domain.mapping.component.LookupComponent
 
import oracle.odi.domain.runtime.scenario.OdiScenario
import oracle.odi.domain.adapter.project.IKnowledgeModule.ProcessingType
import oracle.odi.domain.model.OdiDataStore
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition
import oracle.odi.core.OdiInstance
import oracle.odi.core.config.OdiInstanceConfig
import oracle.odi.generation.support.OdiScenarioGeneratorImpl
import oracle.odi.generation.IOdiScenarioGenerator
 
import oracle.odi.domain.mapping.MappingDataType
 
import oracle.odi.interfaces.interactive.support.InteractiveInterfaceHelperWithActions;
 
def projectName = 'GASSCO_ETL'
def folderName = 'IDC'
def ikmName = 'IKM SQL Control Append (Exadata)'
 
def file = new File('interfaces.txt')
def s = 0
 
String odiSupervisorUser = "SUPERVISOR";
String odiSupervisorPassword = "welcome1";
 
String masterRepositoryJdbcUrl = "jdbc:oracle:thin:@EC2AMAZ-SSSUNUR:1521/orcl";
String masterRepositoryJdbcDriver = "oracle.jdbc.OracleDriver";
String masterRepositoryJdbcUser = "DEV_ODI_REPO";
String masterRepositoryJdbcPassword = "welcome1";
String workRepositoryName = "WORKREP";
 
// Respository and ODI Instance
 
MasterRepositoryDbInfo mRepDbInfo= new MasterRepositoryDbInfo(masterRepositoryJdbcUrl, masterRepositoryJdbcDriver, masterRepositoryJdbcUser, masterRepositoryJdbcPassword.toCharArray(), new PoolingAttributes());
WorkRepositoryDbInfo wRepDbInfo= new WorkRepositoryDbInfo(workRepositoryName, new PoolingAttributes());
OdiInstance odiInstance = OdiInstance.createInstance(new OdiInstanceConfig(mRepDbInfo, wRepDbInfo));
 
// Authentication
 
Authentication auth = odiInstance.getSecurityManager().createAuthentication(odiSupervisorUser, odiSupervisorPassword.toCharArray());
odiInstance.getSecurityManager().setCurrentThreadAuthentication(auth);
 
// Transaction Instance
 
ITransactionDefinition txnDef = new DefaultTransactionDefinition();
ITransactionManager tm = odiInstance.getTransactionManager();
ITransactionStatus txnStatus = tm.getTransaction(txnDef);
 
enum MatchTypes {EQUALS,SRCENDSWITH, TGTENDSWITH, SRCSTARTSWITH, TGTSTARTSWITH}
enum MatchCaseTypes {MATCH,IGNORECASE}
 
def createExpressions(component, conPoint, matchType, matchCaseType) {
    atts = null
    if (conPoint != null)   atts = conPoint.getUpstreamInScopeAttributes()
    else atts = component.getUpstreamLeafAttributes(component)
 
    tatts = component.getAttributes()
    for (MapAttribute tgt_attr : tatts) {
        attr_str = tgt_attr.getName()
 
        if (matchCaseType == MatchCaseTypes.IGNORECASE) {
            attr_str = attr_str.toLowerCase()
    }
 
    sourceCol = null;
 
    for (MapAttribute src_attr : atts) {
        src_attr_str = src_attr.getName()
        if (matchCaseType == MatchCaseTypes.IGNORECASE) {
            src_attr_str = src_attr_str.toLowerCase()
        }
        if ( (matchType == MatchTypes.SRCENDSWITH && src_attr_str.endsWith( attr_str )) ||
           (matchType == MatchTypes.SRCSTARTSWITH && src_attr_str.startsWith( attr_str )) ||
           (matchType == MatchTypes.TGTSTARTSWITH && attr_str.startsWith( src_attr_str )) ||
           (matchType == MatchTypes.TGTENDSWITH && attr_str.endsWith( src_attr_str )) ||
           (matchType == MatchTypes.EQUALS && attr_str.equals( src_attr_str )) ) {
 
        sourceCol = src_attr
        break
 
        }
    }
    if (sourceCol != null && conPoint != null)  tgt_attr.setExpression( conPoint, sourceCol, null )
    else if (sourceCol != null)  tgt_attr.setExpression( sourceCol )
  }
}
 
file.eachLine { line ->
    s++
    toks = line.split(",")
 
    if (toks.length != 6)
    {
        println("Error in input, line: "+s)
    }
    else
    {
        mappingname = toks[0]
        sourcemodel = toks[1]
        sourcetable = toks[2]
        targetmodel = toks[3]
        targettable = toks[4]
        activeyn    = toks[5]
        
        if (activeyn == 'Y') 
        {
         
             println(mappingname)
            // Find the folder
            Collection fold = ((IOdiFolderFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiFolder.class)).findByName(folderName);
            for (Iterator it = fold.iterator(); it.hasNext();) {
              folder = (OdiFolder) it.next();
            }

            // Create the Target Table object
            OdiDataStore targetDatastore = ((IOdiDataStoreFinder)odiInstance.getTransactionalEntityManager().
                  getFinder(OdiDataStore.class)).findByName(targettable, targetmodel);

            // Create the Source Table object
            sourceDatastore = ((IOdiDataStoreFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(sourcetable, sourcemodel);

            // Create Mapping Itself
            map = new Mapping(mappingname, folder)
            odiInstance.getTransactionalEntityManager().persist(map);

            // Add a Data Set
            Dataset dataSet = (Dataset) map.createComponent("DATASET","DEFAULTDATASET");


            OdiDataStore odiDatastore1 = ((IOdiDataStoreFinder)odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(sourcetable, sourcemodel);
            DatastoreComponent dsc = (DatastoreComponent) dataSet.addSource(odiDatastore1, false);

            targetDatastore = ((IOdiDataStoreFinder)odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(targettable, targetmodel);
            targetDatastoreC  = (DatastoreComponent) map.createComponent("DATASTORE",targetDatastore, false);

            // Connect Data Set to Target
            dataSet.connectTo(targetDatastoreC);

            // Automap Columns
            createExpressions(targetDatastoreC, null ,MatchTypes.EQUALS,MatchCaseTypes.MATCH);

            // IKM Setting
            map.createPhysicalDesign('Default')
            physicalDesign  = map.getPhysicalDesign(0)
            node = physicalDesign.findNodeByName(targettable)

            foundKMs = ((IOdiIKMFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiIKM.class)).findByName(ikmName,projectName);
            def km = foundKMs[0]
            node.setIKM(km)

            // Set KM Options
            ikmOptions = node.getIKMOptionValues()
            ikmOptions.each {ikmOption ->
              switch (ikmOption.getName())
              {
                //case 'USE_CREATE_TABLE_FOR_APPEND': ikmOption.setValue('True')
                case 'FLOW_CONTROL': ikmOption.setValue(1)
                case 'DELETE_TEMPORARY_OBJECTS': ikmOption.setValue(0)
              }
            }
        }    
 
    }
    }
 
  odiInstance.getTransactionManager().commit(txnStatus);
  odiInstance.close();