@Grab(group='com.jcraft', module='jsch', version='0.1.55')
@Grab(group='org.postgresql', module='postgresql', version='42.2.8')
@Grab(group='org.codehaus.groovy', module='groovy-sql', version='3.0.7')

import groovy.sql.Sql
import groovy.sql.Sql.newInstance
import groovy.sql.Sql.withInstance
import groovy.json.JsonSlurper
import groovy.time.TimeCategory

import java.sql.*
import com.jcraft.jsch.JSch
import com.jcraft.jsch.Session


class Deploy {

    // Done
    def pause() {
        def key = System.console().readLine("Press any key to exit")
    }

    // Done (?)
    def setupLogging(String PROD, String ENV) {

        checkParameter("PROD", PROD)
        checkParameter("ENV", ENV)

        def dtm = new Date().format("yyyyMMddHHmmss")
        def logname = "${prod}_${env}_deployall_${dtm}.txt"
        def logfile = new File("log", logname)
        logfile.text = ""
        logfile << "Logging started at: ${new Date()}\n"
        return dtm
    }

    // Done
    def readConfig() {
        def config = new JsonSlurper().parse(new File('config/config.json').text)
        return config
    }

    // Done
    def moveLogToArchive() {
        def sourceDir = new File("${File.separator}${System.getProperty('user.dir')}", 'log')
        
        def destDir = new File("${File.separator}${System.getProperty('user.dir')}", "log${File.separator}archive")

        log_files = sourceDir.listFiles();

        for (File f in files){
            if (f.isFile() && f.name.endsWith(".txt")){
                // println f
                def newContent = new File(sourceDir, f).text
                new File(destDir, f).text = newContent
            }
        }

    }

    // Done
    def moveSqlToFolder(String PROD, String ENV, def DTM, String DEPLOY_PATH) {
        checkParameter("PROD", PROD)
        checkParameter("ENV", ENV)
        checkParameter("DTM", DTM)
        checkParameter("DEPLOY_PATH", DEPLOY_PATH)

        def foldPath = new File("${File.separator}${deployPath}${prod}-${env}-${dtm}")

        if (!foldPath.exists()){
            foldPath.mkdirs()
        }
        
        for (File file in deployPath.listFiles()){
            if (file.isFile() && file.name.endsWith(".sql")){
                Files.copy("${File.separator}${deployPath}${file}", "${File.separator}${foldPath}${file}" StandardCopyOption.REPLACE_EXISTING)
            }
        }
    }

    // Pending
    def moveFolderToArchive(String DEPLOY_PATH) {
        checkParameter("DEPLOY_PATH", DEPLOY_PATH)

        deployPath.eachFile { file ->
           file.listFiles()?.findAll { it.isDirectory() }
           file.listFiles()?.findAll { it.isFile() }
        }


        new File(deployPath).eachFile { file ->
            if (file.isDirectory() && !file.name.contains("archive")) {
                def dest = new File(deployPath, "archive")
                file.renameTo(new File(dest, file.name))
            }
        }
    }

    // Done
    def openSshTunnel(def PSQL_DETAILS) {
        checkParameter("PSQL_DETAILS", PSQL_DETAILS)
        
        JSch jsch = new JSch();
        Session session = jsch.getSession(
            PSQL_DETAILS['ssh_host'],
            PSQL_DETAILS['ssh_user'],
            PSQL_DETAILS['ssh_port']
        );
        session.setConfig("StrictHostKeyChecking", "no");
        jsch.addIdentity(PSQL_DETAILS['ssh_pvk']);
        session.connect();
        return session;
    }

    // Done
    def closeSshTunnel(def SSH_TUNNEL = null) {
        checkParameter("SSH_TUNNEL", SSH_TUNNEL)

        if (SSH_TUNNEL != null) {
            SSH_TUNNEL.disconnect()
        }
    }

    // Done
    def dbConn(def PSQL_DETAILS, String DATABASE, String HOSTNAME, def SSH_TUNNEL = null) {
        checkParameter("PSQL_DETAILS", PSQL_DETAILS)
        checkParameter("DATABASE", DATABASE)
        checkParameter("HOSTNAME", HOSTNAME)
        checkParameter("SSH_TUNNEL", SSH_TUNNEL)

        if (SSH_TUNNEL != null) {
            def pgHost = '127.0.0.1'
            def pgPort = SSH_TUNNEL != null ? SSH_TUNNEL.localBindPort : 7432
        }
        else{
            def pgHost = HOSTNAME
            def pgPort = 7432
        }

        def conn = Sql.newInstance(
            "jdbc:postgresql://${pgHost}:${pgPort}/${DATABASE}",
            PSQL_DETAILS['user'],
            PSQL_DETAILS['passwd'],
            "org.postgresql.Driver"
        )

        conn.connection.autoCommit = true
        return conn
    }

    // Done
    def getCredentials(def CONFIG_DATA, String ENV, String PROD) {

        checkParameter("CONFIG_DATA", CONFIG_DATA)
        checkParameter("ENV", ENV)
        checkParameter("PROD", PROD)

        def psqlDetails = [:]
        psqlDetails['support_host'] = CONFIG_DATA[PROD]['env'][ENV]['support_host']
        psqlDetails['log_host'] = CONFIG_DATA[PROD]['env'][ENV]['log_host']
        psqlDetails['archival_host'] = CONFIG_DATA[PROD]['env'][ENV]['archival_host']
        psqlDetails['tenant_host'] = CONFIG_DATA[PROD]['env'][ENV]['tenant_host']  
        psqlDetails['postfix'] = CONFIG_DATA[PROD]['env'][ENV]['postfix']

        psqlDetails['port'] = CONFIG_DATA[PROD]['env'][ENV]['port']
        psqlDetails['user'] = CONFIG_DATA[PROD]['env'][ENV]['user']
        psqlDetails['passwd'] = CONFIG_DATA[PROD]['env'][ENV]['pass']
        psqlDetails['env_type'] = CONFIG_DATA[PROD]['env'][ENV]['env_type']
        isSshRequired = CONFIG_DATA[PROD]['env'][ENV]['is_ssh_required']

        if isSshRequired:
            psqlDetails['ssh_host'] = CONFIG_DATA[PROD]['env'][ENV]['ssh_host']
            psqlDetails['ssh_port'] = CONFIG_DATA[PROD]['env'][ENV]['ssh_port']
            psqlDetails['ssh_user'] = CONFIG_DATA[PROD]['env'][ENV]['ssh_username']
            psqlDetails['ssh_pvk'] = CONFIG_DATA[PROD]['env'][ENV]['ssh_private_key']

        return psqlDetails
    }

    // Done
    def getDbTypeSearch(String PROD, String DB_NAME) {

        checkParameter("PROD", PROD)
        checkParameter("DB_NAME", DB_NAME)

        def dbtypeSearch
        if (DB_NAME.contains("Tenant") && PROD == 'cep'){
            dbtypeSearch = 'CygnetGSPTenant'
        }
        else if(DB_NAME.contains("Tenant") && PROD == 'cgsp'){
            dbtypeSearch = 'CygnetGSPPassthroughTenant'
        }
        else if(DB_NAME.contains("Archival") && PROD == 'cep'){
            dbtypeSearch = 'CygnetGSPArchival'
        }
        else if(DB_NAME.contains("Archival") && PROD == 'cgsp'){
            dbtypeSearch = 'CygnetGSPPassthroughArchival'
        }
        else{
            dbtypeSearch = DB_NAME
        }

        return dbtypeSearch
    }

    // Done
    def printDbStatus(String DB, def STATUS) {

        checkParameter("DB", DB)
        checkParameter("STATUS", STATUS)

        def result = STATUS
        if (result == false ) {
            println "Database : ${DB} Result : Fail"
        } else {
            println result 
            println "Database : ${DB} Result : Success"

            while (result != null) {
                println("SQL result: $result")
                result = result.getNextWarning()
            }
        }
        
    }

    // 
    def executeAll(String PROD, def PSQL_DETAILS, String DEPLOY_PATH, def SSH_TUNNEL = null) {

        checkParameter("PROD", PROD)
        checkParameter("PSQL_DETAILS", PSQL_DETAILS)
        checkParameter("DEPLOY_PATH", DEPLOY_PATH)
        checkParameter("SSH_TUNNEL", SSH_TUNNEL)

        for(File file in DEPLOY_PATH.listFiles()){
            if (file.name.endsWith(".sql")){
                sql = "${DEPLOY_PATH}${File.seperator}${file}"
                dbname = file.substring(3,file.indexOf("-"))
                if(dbname.contains("Tenant")){
                    def hostName = PSQL_DETAILS['tenant_host']
                    def dbConn = dbConn(PSQL_DETAILS, "postgres", hostName, sshTunnel)
                    def dbTypeSearch = getDbTypeSearch(PROD, dbname)
                    def dblist = getDbList(dbConn, dbTypeSearch)
                    println("------Before-----${dblist}")
                    dblist = dblist.findAll{ element ->
                        element.contains(PSQL_DETAILS['postfix'])
                    }
                    println("------After-----${dblist}")

                    dbConn.close()

                    for (){
                        
                    }
                    
                }
                else if(dbname.contains("Archival")){
                    def hostName = PSQL_DETAILS['archival_host']
                    def dbConn = dbConn(PSQL_DETAILS, "postgres", hostName, SSH_TUNNEL)
                    def dbTypeSearch = getDbTypeSearch(prod, dbname)
                    def dblist = getDbList(dbConn, dbTypeSearch)
                    println("------Before-----${dblist}")
                    dblist = dblist.findAll{ element ->
                        element.contains(PSQL_DETAILS['postfix'])
                    }
                    println("------After-----${dblist}")

                    dbConn.close()

                    for (){
                        
                    }
                    
                }
                else if(dbname.contains("Log")){
                    def hostName = PSQL_DETAILS['log_host']
                    dbname = "${dbname}_${PSQL_DETAILS['postfix]}"
                    // get list of log dbs
                    def dbConn = dbConn(PSQL_DETAILS, dbname, hostName, SSH_TUNNEL)
                    def status = executeQuery(dbConn, sql)
                    printDbStatus(dbname, status)

                    dbConn.close()

                }
                else{
                    def hostName = PSQL_DETAILS['support_host']
                    dbname = "${dbname}_${PSQL_DETAILS['postfix]}"
                    // get list of support dbs
                    def dbConn = dbConn(PSQL_DETAILS, dbname, hostName, SSH_TUNNEL)
                    def status = executeQuery(dbConn, sql)
                    printDbStatus(dbname, status)

                    dbConn.close()
                }
            }
        }

        // new File(deployPath).eachFile { file ->
        //     if (file.name.endsWith(".sql")) {
        //         def sql = new File(deployPath, file.name)
        //         def dbname = file.name[3..(file.name.indexOf('-') - 1)]

        //         if (dbname.contains("Tenant")) {
        //             def hostName = psqlDetails['tenantHost']
        //             def dbConn = dbConn(psqlDetails, "postgres", hostName, sshTunnel)
        //             def dbTypeSearch = getDbTypeSearch(prod, dbname)
        //             def dblist = getDbList(dbConn, dbTypeSearch)
        //             println "-------Before------ ${dblist}"

        //             dblist = dblist.findAll { it.contains(psqlDetails['postfix']) }
        //             println "-------After------ ${dblist}"

        //             dbConn.close()

        //             dblist.each { db ->
        //                 db = db.toString()
        //                 println db
        //                 dbConn = dbConn(psqlDetails, db, hostName, sshTunnel)
        //                 def status = executeQuery(dbConn, sql)
        //                 printDbStatus(db, status)
        //                 dbConn.close()
        //             }
        //         } 
        //     }
        // }
    }

    // Done
    def getDbList(def CONN, String DB_TYPE) {

        checkParameter("CONN", CONN)
        checkParameter("DB_TYPE",DB_TYPE)

        try {
            // def cursor = conn.createStatement()
            def query = "SELECT datname FROM pg_database WHERE datname ILIKE '${DB_TYPE}'"
            // def rs = cursor.executeQuery(query)
            // def dblist = []
            // while (rs.next()) {
            //     dblist << rs.getString(1)
            // }
            // cursor.close()

            dblist = CONN.rows(query)
            CONN.close()
            return dblist
        } catch (Exception e) {
            println("Error occurred")
            println("Exception: ${e.message}")
            e.printStackTrace()
            System.exit(1)
        }
    }

    // Done
    def executeQuery(def CONN, String FILE) {

        checkParameter("CONN", CONN)
        checkParameter("FILE",FILE)
        
        try {
            // def cursor = conn.createStatement()
            CONN.execute(new File(FILE).text)
            SQLWarning warning = CONN.getWarnings()
            CONN.close()
            return warning
        } catch (Exception e) {
            println("Error occurred")
            println("Exception: ${e.message}")
            e.printStackTrace()
            return false
        }
    }
}

// Done
def main() {
    def parser = new groovy.cli.OptionParser()
    parser.accepts('prod', 'Product').withRequiredArg().ofType(String)
    parser.accepts('env', 'Environment').withRequiredArg().ofType(String)
    parser.accepts('parallel', 'Parallel execution').withOptionalArg().ofType(Boolean)

    def options = parser.parse(args)
    def prod = options.valueOf('prod')
    def env = options.valueOf('env')
    def parallel = options.has('parallel')

    // 
    def obj = new Deploy()
    def deployPath = new File("${File.separator}${System.getProperty('user.dir')}", 'deploy')

    if (!parallel) {
        println 'Moving old log files to archive'
        obj.moveLogToArchive()
    }

    println 'Setup logging'
    def dtm = obj.setupLogging(prod, env)

    if (!parallel) {
        println 'Move older deploy folder to archive'
        obj.moveFolderToArchive(deployPath)
    }

    println 'Reading config'
    def configData = obj.readConfig()

    def availableEnv = configData[prod]['env'].keySet()
    def availableEnvMsg = "Invalid environment option for chosen product (${prod}). Available environments: ${availableEnv}"
    
    if (env in availableEnv) {
        // Do nothing, valid environment
    } else {
        println(availableEnvMsg)
        System.exit(1)
    }

    def initMsg = "Changes will be deployed on product : ${prod.toUpperCase()} & environment : ${env.toUpperCase()}."
    println initMsg

    def key = System.console().readLine("Do you want to Continue (Y/N) : ")
    if (key == "Y") {
        // Do nothing, continue execution
    } else {
        println "Did not receive confirmation. Aborting..."
        System.exit(1)
    }

    println 'Execution : Start'
    def psqlDetails = obj.getCredentials(configData, env, prod)
    if ('ssh_host' in psqlDetails) {
        def sshTunnel = obj.openSshTunnel(psqlDetails)
        obj.executeAll(prod, psqlDetails, deployPath, sshTunnel)
        obj.closeSshTunnel(sshTunnel)
    } else {
        obj.executeAll(prod, psqlDetails, deployPath)
    }
    println 'Execution : End'

    println 'Move release files to folder'
    obj.moveSqlToFolder(prod, env, dtm, deployPath)

    obj.pause()
}

try {
    main()
} catch (Exception e) {
    println('Unhandled error occurred')
    println(e)
    def key = System.console().readLine("Press any key to exit")
}


