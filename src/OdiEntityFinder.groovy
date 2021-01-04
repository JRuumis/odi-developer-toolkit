

import oracle.odi.core.OdiInstance
import oracle.odi.core.persistence.IOdiEntityManager

import oracle.odi.domain.model.OdiModel
import oracle.odi.domain.model.OdiDataStore
import oracle.odi.domain.model.finder.IOdiDataStoreFinder
import oracle.odi.domain.model.finder.IOdiModelFinder

import oracle.odi.domain.mapping.Mapping
import oracle.odi.domain.mapping.finder.IMappingFinder

import oracle.odi.domain.project.OdiFolder
import oracle.odi.domain.project.finder.IOdiProjectFinder
import oracle.odi.domain.project.finder.IOdiPackageFinder
import oracle.odi.domain.project.finder.IOdiUserProcedureFinder
import oracle.odi.domain.project.finder.IOdiVariableFinder
import oracle.odi.domain.project.finder.IOdiFolderFinder
import oracle.odi.domain.project.finder.IOdiIKMFinder
import oracle.odi.domain.project.finder.IOdiLKMFinder
import oracle.odi.domain.project.OdiIKM
import oracle.odi.domain.project.OdiLKM

import oracle.odi.domain.topology.finder.IOdiLogicalSchemaFinder
import oracle.odi.domain.topology.finder.IOdiContextFinder
import oracle.odi.domain.topology.OdiLogicalSchema
import oracle.odi.domain.topology.OdiContext
import oracle.odi.domain.topology.finder.IOdiTechnologyFinder
import oracle.odi.domain.topology.OdiTechnology

import oracle.odi.domain.runtime.scenario.OdiScenario
import oracle.odi.domain.runtime.scenario.finder.IOdiScenarioFinder
import oracle.odi.domain.runtime.loadplan.OdiLoadPlan
import oracle.odi.domain.runtime.loadplan.finder.IOdiLoadPlanFinder

import oracle.odi.domain.project.OdiProject
import oracle.odi.domain.project.OdiPackage
import oracle.odi.domain.project.OdiUserProcedure
import oracle.odi.domain.project.OdiVariable



class OdiEntityFinder {

    OdiEntityFinder(odiInstance) {
        this.odiInstance = odiInstance
        this.entityManager = this.odiInstance.getTransactionalEntityManager()

        this.packageFinder = (IOdiPackageFinder) this.entityManager.getFinder(OdiPackage.class)
        this.modelFinder = (IOdiModelFinder) this.entityManager.getFinder(OdiModel.class)
        this.procedureFinder = (IOdiUserProcedureFinder) this.entityManager.getFinder(OdiUserProcedure.class)
        this.variableFinder = (IOdiVariableFinder) this.entityManager.getFinder(OdiVariable.class)
        this.contextFinder = (IOdiContextFinder) this.entityManager.getFinder(OdiContext.class)
        this.odiTechnologyFinder = (IOdiTechnologyFinder) this.entityManager.getFinder(OdiTechnology.class)
        this.logicalSchemaFinder = (IOdiLogicalSchemaFinder) this.entityManager.getFinder(OdiLogicalSchema.class)
        this.loadPlanFinder = (IOdiLoadPlanFinder) this.entityManager.getFinder(OdiLoadPlan.class)
        this.scenarioFinder = (IOdiScenarioFinder) this.entityManager.getFinder(OdiScenario.class)
        this.folderFinder = (IOdiFolderFinder) this.entityManager.getFinder(OdiFolder.class)
        this.mappingFinder = (IMappingFinder) this.entityManager.getFinder(Mapping.class)
        this.dataStoreFinder = (IOdiDataStoreFinder)this.entityManager.getFinder(OdiDataStore.class)
        this.projectFinder = (IOdiProjectFinder)this.entityManager.getFinder(OdiProject.class)
        this.ikmFinder = (IOdiIKMFinder) this.entityManager.getFinder(OdiIKM.class)
        this.lkmFinder = (IOdiLKMFinder) this.entityManager.getFinder(OdiLKM.class)

    }

    private OdiInstance odiInstance
    private IOdiEntityManager entityManager

    def packageFinder
    def modelFinder
    def procedureFinder
    def variableFinder
    def contextFinder
    def odiTechnologyFinder
    def logicalSchemaFinder
    def loadPlanFinder
    def scenarioFinder
    def folderFinder
    def mappingFinder
    def dataStoreFinder
    def projectFinder
    def ikmFinder
    def lkmFinder
}