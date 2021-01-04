

import oracle.odi.core.persistence.transaction.ITransactionManager
import oracle.odi.core.persistence.transaction.ITransactionStatus
import oracle.odi.core.persistence.transaction.support.DefaultTransactionDefinition
import oracle.odi.core.persistence.IOdiEntityManager
import oracle.odi.core.OdiInstance

import oracle.odi.domain.IOdiEntity
import oracle.odi.domain.runtime.scenario.OdiScenario

import oracle.odi.generation.support.OdiScenarioGeneratorImpl
import oracle.odi.generation.IOdiScenarioGenerator



class OdiAccess {

    private OdiInstance odiInstance
    OdiEntityFinder finder

    OdiAccess(OdiInstance odiInstance) {
        this.odiInstance = odiInstance
        this.finder = new OdiEntityFinder(odiInstance)
        this.initTransaction()
    }
    // todo: constructor without the odiInstance parameter for standalone

    private ITransactionManager transactionManager
    private IOdiEntityManager transactionalEntityManager
    private DefaultTransactionDefinition transactionDefinition
    private ITransactionStatus transactionStatus

    private def initTransaction () {
        this.transactionManager = odiInstance.getTransactionManager()
        this.transactionalEntityManager = odiInstance.getTransactionalEntityManager()
        this.transactionDefinition = new DefaultTransactionDefinition()
        this.transactionStatus = this.transactionManager.getTransaction(this.transactionDefinition)
    }

    def persistEntity(IOdiEntity entity) {
        this.transactionalEntityManager.persist(entity)
    }

    def saveEntity(IOdiEntity entity) {
        this.transactionalEntityManager.persist(entity)
        this.transactionManager.commit(transactionStatus)
        this.initTransaction()
    }

    def deleteEntity(IOdiEntity entity) {
        this.transactionalEntityManager.remove(entity)
        this.transactionManager.commit(transactionStatus)
        this.initTransaction()
    }

    def commit() {
        this.transactionManager.commit(transactionStatus)
        this.initTransaction()
    }

    def commitAndClose() { // Close will end the ODI Session in ODI Studio. Don't use this with the built in session connection!
        this.transactionManager.commit(transactionStatus)
        this.odiInstance.close()
    }

    def createAndSaveScenario(IOdiEntity odiEntity, String scenarioName) {
        String normalisedScenarioName = this.normaliseScenarioName(scenarioName)
        IOdiScenarioGenerator scenarioGenerator = new OdiScenarioGeneratorImpl(this.odiInstance)
        OdiScenario scenario = scenarioGenerator.generateScenario(odiEntity, normalisedScenarioName, "001")
        this.saveEntity(scenario)
        return scenario
    }

    def normaliseScenarioName(String inputName) {
        return inputName
                .replace('.','_')
                .replace(' ','_')
                .replace('-','_')
                .replace('{','_')
                .replace('}','_')
                .toUpperCase()
    }
}