import java.text.SimpleDateFormat

class ConsoleLogger {

    ConsoleLogger(master, LoggingLevel lvl) {
        this.nrOfTabs = 0
        this.loggingLevel = lvl
        this.masterScript = master
    }

    //Script masterScript
    def masterScript // in ODI Studio, this is not recognised as Script

    private int nrOfTabs
    private LoggingLevel loggingLevel
    private String tab = "    "

    private def tabs() { tab * nrOfTabs }

    def currentTimestamp() {
        def currentDatetime = new Date()
        def datetimeFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
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

    private def drawSplitter(String splitterChar) {
        consolePrint( this.tabs() +  (splitterChar * (100 - this.tabs().size())) )
    }

    def splitter() { this.drawSplitter("-") }
    def strongSplitter() { this.drawSplitter("=") }

    def splitterDebug() {
        if (this.loggingLevel == LoggingLevel.DEBUG)
            this.drawSplitter("-")
    }

    enum LoggingLevel {NORMAL, DEBUG}
}