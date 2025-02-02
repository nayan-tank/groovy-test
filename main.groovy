def dtm = new Date().format("yyyyMMddHHmmss")
def logname = "${prod}_${env}_deployall_${dtm}.txt"
def logfile = new File("log", logname)
logfile.text = ""
logfile << "Logging started at: ${new Date()}\n"