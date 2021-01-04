import java.text.SimpleDateFormat

enum LoggingLevel {NORMAL, DEBUG}

println ("Type of script: ${this.getClass()}")
println ("Type of super: ${this.getClass().getSuperclass()}")
println ("Is Script?: ${this instanceof Script}")

class ConsoleLogger {

    Script masterScript

    ConsoleLogger(Script master, LoggingLevel lvl) {
        this.nrOfTabs = 0
        this.loggingLevel = lvl
        this.masterScript = master
    }

    private int nrOfTabs
    private LoggingLevel loggingLevel
    private String tab = "   "

    private def tabs() { tab * nrOfTabs }

    private def timeStamp() {
        def currentDatetime = new Date()
        def datetimeFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")
        return "[${datetimeFormat.format(currentDatetime)}]"
    }

    def tabIncrease() { this.nrOfTabs++ }
    def tabDecrease() { this.nrOfTabs-- }
    def tabZero() { this.nrOfTabs = 0 }

    private def consolePrint(String msg) { this.masterScript.println(msg) }

    def error(String msg) {
        consolePrint( this.tabs() + "[ERROR]${this.currentTimestamp()} ${msg}" )
    }

    def warning(String msg) {
        consolePrint( this.tabs() + "[warning]${this.currentTimestamp()} ${msg}" )
    }

    def info(String msg) {
        consolePrint( this.tabs() + "[info]${this.currentTimestamp()} ${msg}" )
    }

    def debug(String msg) {
        if (this.loggingLevel == LoggingLevel.DEBUG) {
            consolePrint( this.tabs() + "[debug]${this.currentTimestamp()} ${msg}" )
        }
    }

    def splitter() {
        consolePrint( this.tabs() +  ("-" * (100 - this.tabs().size())) )
    }

}

ConsoleLogger logger = new ConsoleLogger(this, LoggingLevel.DEBUG)



// testing logger...
logger.info("testing info")
logger.warning("testing warnings")
logger.splitter()
logger.error("testing errors")
logger.tabIncrease()
logger.info("testing tabs")
logger.tabIncrease()
logger.info("more tabs")
logger.tabIncrease()
logger.splitter()
logger.info("even more tabs")
logger.debug("DEBUG MESSAGE - only visible when logging mode set to DEBUG")
logger.debug("DEBUG MESSAGE - one more!!!")
logger.tabDecrease()
logger.info("decrease tabs")
logger.tabZero()
logger.info("zero tabs")
logger.splitter()

