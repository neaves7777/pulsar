package pulsar;

 
import java.io.FileInputStream;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import org.ini4j.Wini;
import com.ibm.as400.access.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Enumeration;

//public class pulsar {
 
//    public pulsar(String connName) {

public class pulsar {
    private Connection connection;
    private String library;
    private String ionlib;
    private String prodlib;
    private String svmlib;
    private String ollib;
    private String colib;
    private String[] DRIVERS;
    private String[] URLS;
    private Integer dbtype;  // 0 -  Oracle  1 - sql server  2 - db2
    private String[] dbdate;
    private String[] connsep;
    private String[] portsep;
    private String[] qualsep;
    
    private String host;
    private String userId;
    private String password;
    private Properties props;
    private Wini ini;
    private String connName;

    @SuppressWarnings("oracle.jdeveloper.java.tag-is-missing")
    public pulsar(String connectionName) {
        connName = connectionName;
        props = new Properties();
        
        DRIVERS = new String[3];
        DRIVERS[0] = "oracle.jdbc.driver.OracleDriver"; 
        DRIVERS[1] = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
        DRIVERS[2] = "com.ibm.as400.access.AS400JDBCDriver";
        
        URLS = new String[3];
        URLS[0] = "jdbc:oracle:thin:@";
        URLS[1] = "jdbc:sqlserver://";
        URLS[2] = "jdbc:as400://";
        
        dbdate = new String[3];
        dbdate[0] = "sysdate";
        dbdate[1] = "getdate()";
        dbdate[2] = "current timestamp";
        
        connsep = new String[3];
        connsep[0] = ":";
        connsep[1] = ";";
        connsep[2] = ";";
        
        portsep = new String[3];
        portsep[0] = ":";
        portsep[1] = ":";
        portsep[2] = ";";
        
        qualsep = new String[3];
        qualsep[0] = ".";
        qualsep[1] = ".";
        qualsep[2] = "/";
      
        try {
            String iniFile = "pulsar.ini";
            // iniFile = "/Users/Admin/jdeveloper/mywork/Pulsar/pulsar/src/pulsar/pulsar.ini";
            ini = new Wini(new FileInputStream(iniFile));
            
            dbtype = ini.get(connName, "dbtype", int.class);
            
            host = ini.get(connName, "host");
            userId = ini.get(connName, "userId");
            password = ini.get(connName, "password");
            
            library = ini.get(connName, "library") + qualsep[dbtype];
            prodlib = ini.get(connName, "prodlib") + qualsep[dbtype];
            svmlib = ini.get(connName, "svmlib") + qualsep[dbtype];
            colib = ini.get(connName, "colib") + qualsep[dbtype];
            ollib = ini.get(connName, "ollib") + qualsep[dbtype];
            ionlib = ini.get(connName, "ionlib") + qualsep[dbtype];
            
            
           String URL = URLS[dbtype] +
           ini.get(connName, "host") + portsep[dbtype] + 
           ini.get(connName, "port") + connsep[dbtype] + 
           ini.get(connName, "database");
           // System.out.println(connName + " " + URL);
             //Connect to iSeries
            Class.forName(DRIVERS[dbtype]);
            connection = DriverManager.getConnection(URL,
                ini.get(connName, "userId"),
                ini.get(connName, "password"));
        }
        catch (Exception e) {
         System.out.println(e);
        }
    }
    
    private Integer to_jde_date( Timestamp ts ) {
        SimpleDateFormat  days_fmt = new SimpleDateFormat("D");
        @SuppressWarnings("deprecation")
        Integer year = ts.getYear();
        Integer days = Integer.parseInt( days_fmt.format( ts ) );
        // System.out.println("Year:" + ( 1000 * (year) + days));
        return 1000 * (year) + days;
    }
    
    private Integer to_jde_time( Timestamp ts ) {
        SimpleDateFormat  time_fmt = new SimpleDateFormat("HHmmss");
        String df = time_fmt.format(ts);
        // System.out.println("De Time is:" + ts);
        return Integer.parseInt(df);
    }


    @SuppressWarnings("oracle.jdeveloper.java.insufficient-catch-block")
    private Timestamp from_jde_date( Integer date, Integer time ) {
        Integer year = (date/1000) + 1900;
        Integer doy = date - (year - 1900)*1000;
        String stime = String.format("%06d", time);
        SimpleDateFormat formatter = new SimpleDateFormat( "yyyy MM dd HH:mm:ss" );
        String timestring = String.format("%d %d %d %s:%s:%s", year, 1, 1, stime.substring(0,2), stime.substring(2,4), stime.substring(4,6));
    
        Calendar c = Calendar.getInstance();
        try {
            c.setTime(formatter.parse(timestring));
            c.add(Calendar.DATE, doy -1 );
        } catch (ParseException e) {
            // TODO
        }
        // System.out.println("From JdeDate: " + formatter.format(c.getTime()));
        Timestamp ts = new java.sql.Timestamp( c.getTime().getTime());
        return ts;
    }


    /**
     * @param process
     * @return
     */
    public Timestamp getLastProcRun(String process)  {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        java.util.Date date= new java.util.Date();
        Timestamp lastRun = new Timestamp(date.getTime());
        String filePath = "jdeprod.lck";
        Path path = Paths.get(filePath);
   
        try {
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
            fileChannel.position(fileChannel.size() - 1); // positions at the end of file

            FileLock lock = fileChannel.lock(); // gets an exclusive lock
            
            Wini savini = new Wini(new FileInputStream(connName));
            // savini = new Wini(new FileInputStream("/Users/Admin/jdeveloper/mywork/Pulsar/pulsar/src/pulsar/" + connName));
            String lastRunTxt = savini.get(connName, process);
            lastRun = new Timestamp(date_fmt.parse(lastRunTxt).getTime());
            fileChannel.close(); // also releases lock

            } catch (IOException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
           
        return lastRun;
    }

    /**
     * @param process
     */
    public void setLastProcRun(String process)  {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        java.util.Date date= new java.util.Date();
        Timestamp lastRun = new Timestamp(date.getTime());
        String filePath = "jdeprod.lck";
        Path path = Paths.get(filePath);

        try {
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
            fileChannel.position(fileChannel.size() - 1); // positions at the end of file

            FileLock lock = fileChannel.lock(); // gets an exclusive lock
            Wini savini = new Wini(new File(connName));
            // savini = new Wini(new File("/Users/Admin/jdeveloper/mywork/Pulsar/pulsar/src/pulsar/" + connName));
            String lastRunTxt = date_fmt.format(lastRun);
            savini.put(connName, process, lastRunTxt);
            savini.store();
            fileChannel.close(); // also releases lock
            
            } catch (IOException e ) {
                e.printStackTrace();
        } catch (Exception e) {
               e.printStackTrace();
        }
    }

    /**
     * @param process
     * @param t
     */
    public void setLastProcRun(String process, Timestamp t)  {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String filePath = "jdeprod.lck";
        Path path = Paths.get(filePath);

        try {
            FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
            fileChannel.position(fileChannel.size() - 1); // positions at the end of file

            FileLock lock = fileChannel.lock(); // gets an exclusive lock
            Wini savini = new Wini(new File( connName));
            // savini = new Wini(new File("/Users/Admin/jdeveloper/mywork/Pulsar/pulsar/src/pulsar/" + connName));
            String lastRunTxt = date_fmt.format(t);
            savini.put(connName, process, lastRunTxt);
            savini.store();
            fileChannel.close(); // also releases lock
            
            } catch (IOException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
    

    public void getWsjCount() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = "select " +
        "count(1) as count, rtrim(jcjobsts) as jobstatus " +
        "from " + svmlib + "f986110 " +
        "where jcjobsts in ('P', 'W') or " +
        "     (jcjobsts in ('E','D') and " +
        "         (jcactdate > ? or (jcactdate = ? and jcacttime > ?))) " +
        "group  by rtrim(jcjobsts)";
       
        // String query = null;
        PreparedStatement stmt = null;
               
        Timestamp lastJobRun = getLastProcRun("wsj_job_count");
        Integer activity_date = to_jde_date(lastJobRun);
        Integer activity_time = to_jde_time(lastJobRun);
       
        
        try {
                stmt = connection.prepareStatement(query);
                stmt.setObject(1, activity_date);
                stmt.setObject(2, activity_date);
                stmt.setObject(3, activity_time);

                ResultSet rs = stmt.executeQuery();
              
                while (rs.next()) {
                    System.out.println(
                    "jobstatus=" + rs.getString("JOBSTATUS").trim() +
                    ",count=" + rs.getInt("COUNT") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("wsj_job_count");
    }
    
    public void getSubmittedJobs() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = "select " +
        "jcactdate, jcacttime, jcjobnbr as jobnbr," +
        "jcprocessid as processid, rtrim(jcexehost) as exehost," +
        "rtrim(jcjobque) as jobqueue, rtrim(jcjobsts) as jobstatus, rtrim(jcenhv) as env," +
        "rtrim(jcuser) as jcuser, jcorghost, jcsbmdate, jcsbmtime, rtrim(jcfndfuf2) as jobname " +
        "from " + svmlib + "f986110 " +
        "where jcactdate > ? " +
        "or (jcactdate = ? and jcacttime > ?) " +
        "order by jcactdate, jcacttime";
       
        // String query = null;
        PreparedStatement stmt = null;
       
        Integer submitted_date;
        Integer submitted_time;
        
        Timestamp lastJobRun = getLastProcRun("submitted_jobs");
        Integer activity_date = to_jde_date(lastJobRun);
        Integer activity_time = to_jde_time(lastJobRun);
       
        // query = String.format(query_pattern, activity_date, activity_date, activity_time);
        // System.out.println(query);
        
        try {
                stmt = connection.prepareStatement(query);
                stmt.setObject(1, activity_date);
                stmt.setObject(2, activity_date);
                stmt.setObject(3, activity_time);

                ResultSet rs = stmt.executeQuery();
              
                while (rs.next()) {
                    activity_date  = rs.getInt("JCACTDATE");
                    activity_time  = rs.getInt("JCACTTIME");
                    submitted_date = rs.getInt("JCSBMDATE");
                    submitted_time = rs.getInt("JCSBMTIME");
                   
                    System.out.println(
                    "activity_time=" + date_fmt.format( from_jde_date(activity_date, activity_time) ) +
                    ",submitted_time=" + date_fmt.format( from_jde_date(submitted_date, submitted_time) ) + 
                    ",jobnbr=" + rs.getInt("JOBNBR") +
                    ",processid=" + rs.getInt("PROCESSID") +
                    ",exehost=" + rs.getString("EXEHOST").trim() +
                    ",user=" + rs.getString("JCUSER").trim() +
                    ",jobqueue=" + rs.getString("JOBQUEUE").trim() +
                    ",jobstatus=" + rs.getString("JOBSTATUS").trim() +
                    ",env=" + rs.getString("ENV").trim() +
                    ",jobname=" + rs.getString("JOBNAME").trim()
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("submitted_jobs", from_jde_date(activity_date, activity_time));
    }
    
    public void getWorkCenterMessages() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("dd-MMM-yy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
        "select count(1) msg_count," +
        "zmdti, rtrim(zmtmpi) zmtmpi, rtrim(zmmsgp) zmmsgp " +
        "from " + prodlib + "f01131m " +
        "where zmdti > %d " +
        "group by zmdti, rtrim(zmtmpi), rtrim(zmmsgp)";
       
        String query = null;
        Statement stmt = null;
       
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("work_center_msg");
        Integer iLastRunDate = to_jde_date(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate);
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    date_t  = rs.getInt("ZMDTI");
                   
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",date_t=" + date_fmt.format( from_jde_date(date_t, 0) ) +
                    ",template_id=" + rs.getString("ZMTMPI") +
                    ",message=" + rs.getString("ZMMSGP") +
                    ",msg_count=" + rs.getInt("MSG_COUNT")
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("work_center_msg");
    }
    
    public void getSubSystemJobs() {
        String query =
        "select count(1) cnt, rtrim(ssexehost) ssexehost, rtrim(sspid) sspid, rtrim(ssvers) ssvers " +
        // "sssbmdate, sssbmtime " +
        "from " + svmlib + "f986113 " +
        "group by ssexehost, sspid, ssvers ";
       
        PreparedStatement stmt = null;
        // System.out.println(query);
       
        try {
                stmt = connection.prepareStatement(query);
                //stmt.setObject(1, activity_date);
                //stmt.setObject(2, activity_date);
                //stmt.setObject(3, activity_time);

                ResultSet rs = stmt.executeQuery();
                
              
                while (rs.next()) {
                    //activity_date  = rs.getInt("SSSBMDATE");
                    //activity_time  = rs.getInt("SSSBMTIME");
                   
                    System.out.println(
                    "exehost=" + rs.getString("SSEXEHOST") +
                    ",pid=" + rs.getString("SSPID") +
                    ",version=" + rs.getString("SSVERS") +
                    ",count=" + rs.getInt("CNT")
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        // setLastProcRun("SUBSYSTEM_JOBS", from_jde_date(activity_date, activity_time));
    }
    
    
    public void getPackageBuilds() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
        "select rtrim(hhpkgname) package_name, rtrim(hhpathcd) path_code," +
        " rtrim(hhdatp)  hhdatp ,  hhblddte build_date, hhbldtme build_time, hhbldsts status " +
        "from " + library + "f96215 " +
        "where hhblddte > %d " +
        "or (hhblddte = %d and hhbldtme >= %d )";
       
        String query = null;
        Statement stmt = null;
        
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("package_builds");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",package_name=" + rs.getString("PACKAGE_NAME") +
                    ",path_code=" + rs.getString("PATH_CODE") +
                    ",hhdatp=" + rs.getString("HHDATP") +
                    ",build_date=" + date_fmt.format( from_jde_date( rs.getInt("BUILD_DATE"), rs.getInt("BUILD_TIME") ))  +
                    ",status=" + rs.getInt("STATUS")
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("package_builds");
    }
    
    public void getSchedulerInfo() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
        "select  sjschjbnm,\n" + 
        "sjschrptnm, sjschver, sjschjbstat\n" + 
        "from "+ library + "f91300\n" + 
        "where sjschjbnm != '*SCHEDULER'\n";
       
        Statement stmt = null;
        
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",job_name=\"" + rs.getString("SJSCHJBNM").trim() +
                    "\",report_name=\"" + rs.getString("SJSCHRPTNM").trim() +
                    "\",version=\"" + rs.getString("SJSCHVER").trim() +
                    "\",status=" + rs.getInt("SJSCHJBSTAT")
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
    
    public void getTableRowCnt(String[] tables) {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("dd-MMM-yy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
        "select count(1) cnt " +
        "from " ;
        String eTable = "";
        
        PreparedStatement stmt = null;
       
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        try {
            for (String table: tables) {
                System.out.println( query + table);
                eTable = table;
                stmt = connection.prepareStatement(query + table);
                
                ResultSet rs = stmt.executeQuery();
              
                rs.next();
                System.out.println(
                    time_fmt.format(current_date) + " " + 
                    "table=" + table +
                    ",count=" + rs.getInt("CNT")
                 );
            }
            
            } catch (SQLException e ) {
                System.out.println(time_fmt.format(current_date) + " " + eTable + " does not exist");
                // e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
    
    public void getOracleBackupReport() {
        String query =
           " SELECT " +
           "    i.instance_name," +
           "    to_char(b.start_time, 'mm/dd/yyyy hh24:mi:ss') start_time," +
           "    b.status," +
           "    b.input_type," +
           "    trunc( b.input_bytes/1024/1024/1024 ) input_gb," +
           "    trunc( b.output_bytes/1024/1024/1024) output_gb," +
           "    trunc( b.input_bytes_per_sec ) input_bytes_per_sec," +
           "    trunc( b.output_bytes_per_sec) output_bytes_per_sec," +
           "    b.time_taken_display" +
           " FROM sys.V_$RMAN_BACKUP_JOB_DETAILS b," +
           "      sys.V_$instance i" +
           " WHERE b.start_time > CAST(? as DATE) " +
           " ORDER BY b.start_time";

       
        PreparedStatement stmt = null;
        
        java.util.Date cdate = new java.util.Date();
        // Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("backup_report");

        try {
                stmt = connection.prepareStatement(query);
                stmt.setTimestamp(1, lastJobRun);
                ResultSet rs = stmt.executeQuery();

                while (rs.next()) {
                    System.out.println(
                    rs.getString("START_TIME") +
                    ",database=" + rs.getString("INSTANCE_NAME") +
                    ",status=" + rs.getString("STATUS") +
                    ",backup_type=" + rs.getString("INPUT_TYPE") +
                    ",elapsed_time=" + rs.getString("TIME_TAKEN_DISPLAY") +
                    ",gb_in=" + rs.getString("INPUT_GB") +
                    ",gb_out=" + rs.getString("OUTPUT_GB") +
                    ",input_rate=" + rs.getString("INPUT_BYTES_PER_SEC") +
                    ",output_rate=" + rs.getString("OUTPUT_BYTES_PER_SEC")
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("backup_report");
    }

    public void getOracleLastBackup() {
        String query =
        "select host_name, instance_name, backup_type, " + 
        "      nvl(incremental_level,0) ilevel, " + 
        "      min(round(sysdate-completion_time,2)) days_since_last_backup " + 
        "from sys.V_$backup_set, sys.V_$instance " + 
        "group by host_name, instance_name, backup_type, nvl(incremental_level,0)";
       
        Statement stmt = null;
               
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);

                while (rs.next()) {
                    System.out.println(
                    "hostname=" + rs.getString("HOST_NAME") +
                    ",database=" + rs.getString("INSTANCE_NAME") +
                    ",backup_type=" + rs.getString("BACKUP_TYPE") +
                    ",incr_level=" + rs.getString("ILEVEL") +
                    ",days_since_last_backup=" + rs.getFloat("DAYS_SINCE_LAST_BACKUP") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("last_backup");
    }

    public void getOracleTps() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
           " select  sum(value) tps " +
           " from sys.v_$sysstat" +
           " where name in ('user commits','transaction rollbacks')";

       
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
        Integer tps1, tps2;      
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                tps1 = rs.getInt("TPS");
                Thread.sleep(60000);
                rs = stmt.executeQuery(query);
                rs.next();
                tps2 = (rs.getInt("TPS") - tps1)/60;
            
                System.out.println(
                    time_fmt.format(current_date) +
                    " tps=" + tps2 );
                
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }

    public void getOracleDbInfo() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
           " select  instance_name, host_name host, version, to_char(startup_time, 'mm/dd/yyyy hh24:mi') startup_time, " +
           "round(sysdate - startup_time,1) days_since_restart, status, archiver, shutdown_pending " +
           " from v$instance" ;
    
        
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
        
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",name=" + rs.getString("INSTANCE_NAME") +
                    ",host=" + rs.getString("HOST") +
                    ",version=" + rs.getString("VERSION") +
                    ",startup_time=" + rs.getString("STARTUP_TIME") +
                    ",Days_since_restart=" + rs.getString("DAYS_SINCE_RESTART") +
                    ",status=" + rs.getString("STATUS") +
                    ",archiver=" + rs.getString("ARCHIVER") +
                    ",shutdown=" + rs.getString("SHUTDOWN_PENDING") 
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleUserCalls() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
           " select  sum(value) calls " +
           " from sys.v_$sysstat" +
           " where name in ('user calls')";

       
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
        Long calls1, calls2;      
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                calls1 = rs.getLong("CALLS");
                Thread.sleep(60000);
                rs = stmt.executeQuery(query);
                rs.next();
                calls2 = (rs.getLong("CALLS") - calls1)/60;
            
                System.out.println(
                    time_fmt.format(current_date) +
                    " usercalls=" + calls2 );
                
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }

    public void getOracleSessions() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "select username,\n" + 
            "       status,\n" + 
            "       sid,\n" + 
            "       serial#,\n" + 
            "       machine,\n" + 
            "       module,\n" + 
            "       NVL(sql_id,'NULL') sql_id\n" + 
            "from sys.v_$session\n" + 
            "where username is not null";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",username=" + rs.getString("USERNAME") +
                    ",status=" + rs.getString("STATUS") +
                    ",sid=" + rs.getString("SID") +
                    ",serial#=" + rs.getString("SERIAL#") +
                    ",machine=" + rs.getString("MACHINE") +
                    ",module=" + rs.getString("MODULE") +
                    ",sql_id=" + rs.getString("SQL_ID") 
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleLocks() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "select    blocking_status,\n" + 
            "          object_name\n" + 
            " from (\n" + 
            "    select s1.username || '@' || s1.machine\n" + 
            "                       || '  ,BLOCKING_SID=' || s1.sid || ',  is blocking '\n" + 
            "                       || s2.username || '@' || s2.machine || ' ( SID=' || s2.sid || ' ) ' AS blocking_status\n" + 
            "    from sys.v_$lock l1, sys.v_$session s1, sys.v_$lock l2, sys.v_$session s2\n" + 
            "    where s1.sid=l1.sid and s2.sid=l2.sid\n" + 
            "    and l1.BLOCK=1 and l2.request > 0\n" + 
            "    and l1.id1 = l2.id1\n" + 
            "    and l2.id2 = l2.id2\n" + 
            "    ), (\n" + 
            "    select distinct owner||'.'|| object_name object_name\n" + 
            "    from sys.dba_objects, sys.v_$lock\n" + 
            "    where object_id = id1 and type='TM' and owner='PRODDTA')";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",table_name=" + rs.getString("TABLE_NAME") +
                    ",blocking_user=" + rs.getString("BLOCKING_STATUS")
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleDBFileStat() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "SELECT trunc(PHYBLKRD * 8 / 1024 /1024 ) read_gb," + 
            "       trunc(PHYBLKWRT * 8 / 1024 /1024 ) write_gb," + 
            "       name " + 
            "FROM sys.V_$FILESTAT f, sys.v_$datafile d " + 
            "where f.file# = d.file#";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",name=" + rs.getString("NAME") +
                    ",read_gb=" + rs.getLong("READ_GB") +
                    ",write_gb=" + rs.getLong("WRITE_GB")
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleDBSegmentStat() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "  SELECT \n" + 
            "    segment_name,\n" + 
            "    object_type,\n" + 
            "    physical_reads\n" + 
            "  FROM ( \n" + 
            "    SELECT \n" + 
            "      owner||'.'||object_name as segment_name,\n" + 
            "      object_type,\n" + 
            "      value as physical_reads\n" + 
            "    FROM sys.v_$segment_statistics\n" + 
            "    WHERE statistic_name in ('physical reads')\n" + 
            "    AND owner != 'SYS'\n" + 
            "    ORDER BY physical_reads DESC\n" + 
            "  )\n" + 
            "  WHERE rownum <=50";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
              
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",segment_name=" + rs.getString("SEGMENT_NAME") +
                    ",object-type=" + rs.getString("OBJECT_TYPE") +
                    ",physical_reads=" + rs.getLong("PHYSICAL_READS") 
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleDBIOStat() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "  select\n" + 
            "     round((sum(decode(metric_name, 'Physical Read Bytes Per Sec' , value,0)))/1024,0)  read_kbps,\n" + 
            "     round((sum(decode(metric_name, 'Physical Read Total Bytes Per Sec' , value,0)))/1024,0) readtotal_kbps,\n" + 
            "     round((sum(decode(metric_name, 'Physical Read Total IO Requests Per Sec' , value,0 ))),1) readtotal_ops,\n" + 
            "     round((sum(decode(metric_name, 'Physical Write Bytes Per Sec' , value,0 )))/1024,0)  write_kbps,\n" + 
            "     round((sum(decode(metric_name, 'Physical Write Total Bytes Per Sec' , value,0 )))/1024,0) writetotal_kbps,\n" + 
            "     round((sum(decode(metric_name, 'Physical Write Total IO Requests Per Sec', value,0 ))),1) writetotal_ops\n" + 
            "   from  sys.v_$sysmetric\n" + 
            "   where metric_name in (\n" + 
            "      'Physical Read Total Bytes Per Sec' ,\n" + 
            "      'Physical Read Bytes Per Sec' ,\n" + 
            "      'Physical Write Bytes Per Sec' ,\n" + 
            "      'Physical Write Total Bytes Per Sec' ,\n" + 
            "      'Physical Write Total IO Requests Per Sec',\n" + 
            "      'Physical Read Total IO Requests Per Sec'\n" + 
            "      )\n" + 
            "   and group_id=2";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
              
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    ",read_kbps=" + rs.getLong("READ_KBPS") +
                    ",readtotal_kbps=" + rs.getLong("READTOTAL_KBPS") +
                    ",readtotal_ops=" + rs.getLong("READTOTAL_OPS") +
                    ",write_kbps=" + rs.getLong("WRITE_KBPS") +
                    ",writetotal_kbps=" + rs.getLong("WRITETOTAL_KBPS") +
                    ",writetotal_ops=" + rs.getLong("WRITETOTAL_OPS") 
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    
    public void getOracleSql() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
        "select " +
        "  sql_id, " +
        "  runtime_mem, " +
        "  sorts, " +
        "  executions, " +
        "  disk_reads, " +
        "  direct_writes, " +
        "  buffer_gets, " +
        "  application_wait_time, " +
        "  user_io_wait_time, " +
        "  rows_processed, " +
        "  command_type, " +
        "  cpu_time, " +
        "  elapsed_time, " +
        "  substr(sql_text,1,200) sql_text, " +
        "  substr(sql_fulltext, instr(sql_fulltext,'FROM',1),50) sql_from " +
        " from v$sql where sql_id not in (select sql_id from v$session where username in ('SYS','SYSTEM','DBSNMP'))";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    ",sql_id=" + rs.getString("SQL_ID") +
                    ",runtime_mem=" + rs.getLong("RUNTIME_MEM") +
                    ",sorts=" + rs.getLong("SORTS") +
                    ",executions=" + rs.getLong("EXECUTIONS") +
                    ",disk_reads=" + rs.getLong("DISK_READS") +
                    ",direct_writes=" + rs.getLong("DIRECT_WRITES") +
                    ",buffer_gets=" + rs.getLong("BUFFER_GETS") +
                    ",application_wait_time=" + rs.getLong("APPLICATION_WAIT_TIME") +
                    ",user_io_wait_time=" + rs.getLong("USER_IO_WAIT_TIME") +
                    ",rows_processed=" + rs.getLong("ROWS_PROCESSED") +
                    ",command_type=" + rs.getInt("COMMAND_TYPE") +
                    ",cpu_time=" + rs.getLong("CPU_TIME") +
                    ",elapsed_time=" + rs.getLong("ELAPSED_TIME") +
                    ",sql_text=" + rs.getString("SQL_TEXT") +
                    ",sql_from=" + rs.getString("SQL_FROM") 
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleTablespace() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
        "SELECT m.tablespace_name tablespace_name, " +
        "    round(max(m.used_percent),1) used_percent, " +
        "    round(max(m.tablespace_size*t.block_size/1024/1024),1) tablespace_size, " +
        "    round(max(m.used_space*t.block_size/1024/1024),1) used_space, " +
        "    round(max((m.tablespace_size-m.used_space)*t.block_size/1024/1024),1) free_space " +
        "FROM  dba_tablespace_usage_metrics m, dba_tablespaces t, dba_data_files d " +
        "WHERE m.tablespace_name=t.tablespace_name " +
        "AND d.tablespace_name=t.tablespace_name " +
        "GROUP BY m.tablespace_name";
        
//            "SELECT tablespace_name, used_space, tablespace_size, round(used_percent,1) used_percent " + 
//            "FROM sys.DBA_TABLESPACE_USAGE_METRICS";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
              
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "tablespace_name=" + rs.getString("TABLESPACE_NAME") +
                    ",used_space=" + rs.getLong("USED_SPACE") +
                    ",tablespace_size=" + rs.getLong("TABLESPACE_SIZE") +
                    ",used_percent=" + rs.getFloat("USED_PERCENT") +
                    ",free_space=" + rs.getLong("FREE_SPACE")
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleWaitEvent() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "select * " + 
            "from (" + 
            "select sn.sid," + 
            "       nvl(sn.username, 'sys') username," + 
            "       '\"'||se.event||'\"' event," + 
            "       se.time_waited," + 
            "       se.average_wait," + 
            "       total_waits " + 
            "from sys.v_$session_event se," + 
            "     sys.v_$session sn " + 
            "where sn.sid=se.sid " + 
            "and se.event != 'SQL*Net message from client' " + 
            "and username NOT IN ('SYS', 'DBSNMP','SYSTEM') " + 
            "ORDER BY AVERAGE_WAIT DESC\n" + 
            ") where rownum < 200";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
              
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "sid=" + rs.getInt("SID") +
                    ",username=" + rs.getString("USERNAME") +
                    ",event=" + rs.getString("EVENT") +
                    ",time_waited=" + rs.getLong("TIME_WAITED") +
                    ",average_wait=" + rs.getLong("AVERAGE_WAIT") +
                    ",total_waits=" + rs.getFloat("TOTAL_WAITS")
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getOracleRowCount() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query =
            "select table_name, round(bytes/1073741824,2) as gb, num_rows\n" + 
            "from (\n" + 
            "select table_name, sum(bytes) bytes, sum(num_rows) num_rows\n" + 
            "from (\n" + 
            "select t.table_name, s.bytes, t.num_rows\n" + 
            "from dba_segments s\n" + 
            "inner join dba_tables t\n" + 
            "on s.segment_name = t.table_name\n" + 
            "and s.owner = t.owner\n" + 
            "and s.owner = 'PRODDTA'\n" + 
            "AND s.SEGMENT_TYPE ='TABLE'\n" + 
            "union\n" + 
            "select  i.table_name, sum(s.bytes) bytes, sum(0) num_rows\n" + 
            "from dba_segments s\n" + 
            "inner join dba_indexes i\n" + 
            "on s.segment_name = i.index_name\n" + 
            "and s.owner = i.owner\n" + 
            "and s.owner = 'PRODDTA'\n" + 
            "AND s.SEGMENT_TYPE ='INDEX'\n" + 
            "group by i.table_name\n" + 
            ") group by table_name\n" + 
            "order by bytes desc\n" + 
            ") where rownum < 50\n";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
              
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "tables_name=" + rs.getString("TABLE_NAME") +
                    ",rowcount=" + rs.getLong("NUM_ROWS") +
                    ",table_size_gb=" + rs.getFloat("GB")
                    );
                }
        } catch (SQLException e ) {
           e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void getSecurityHistory() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
           " select\n" + 
           "    shuser,\n" + 
           "    shevtyp,\n" + 
           "    shusr0,\n" + 
           "    shupmj, shupmt\n" + 
           "from " + library + "f9312\n" + 
           "where shupmj > %d\n" + 
           "or ( shupmj = %d and shupmt > %d )\n";

       
        String query = null;
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("security_history");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "shuser=" + rs.getString("SHUSER").trim() +
                    ",shevtyp=" + rs.getString("SHEVTYP") +
                    ",shusr0=" + rs.getString("SHUSR0").trim() +
                    ",change_date=" + time_fmt.format( from_jde_date( rs.getInt("SHUPMJ"), rs.getInt("SHUPMT") )) 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("security_history");
    }
    
    public void getDisabledUser() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
           "select\n" + 
           "    scuser,\n" + 
           "    sceuser,\n" + 
           "    scmuse,\n" + 
           "    scseclst,\n" + 
           "    scupmj, scupmt\n" + 
           "from "+library + "f98owsec\n" + 
           "where scupmj > %d\n" + 
           "or ( scupmj = %d and scupmt > %d )";
    
        
        String query = null;
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
        
        Timestamp lastJobRun = getLastProcRun("disabled_user");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
        
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
        
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "userid=" + rs.getString("SCUSER").trim() +
                    ",admin=" + rs.getString("SCMUSE").trim() +
                    ",last_security_change=" + time_fmt.format( from_jde_date( rs.getInt("SCSECLST"), 0 ))  +
                    ",enabled=" + rs.getString("SCEUSER").trim()  +
                    ",change_date=" + time_fmt.format( from_jde_date( rs.getInt("SCUPMJ"), rs.getInt("SCUPMT") )) 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("disabled_user");
    }
    
    public void getProxyUser() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query[] = new String[3];
        
        query[0] = 
        "select distinct username, " +
        "       account_status " +
        "from dba_users," + library + "f98owsec " +
        "where username = rtrim(scsecusr) ";
        
        
        query[1] = 
        "select distinct name as username, " +
        "    case is_disabled when 1 then 'LOCKED' else 'OPEN' END as account_status " +
        "from sys.sql_logins," + library + ".f98owsec " +
        "where name = scsecusr " +
        "COLLATE SQL_Latin1_General_CP1_CI_AS ";
            
            
        query[2] =
           "select\n" + 
           "    scuser,\n" + 
           "    sceuser,\n" + 
           "    scmuse,\n" + 
           "    scseclst,\n" + 
           "    scupmj, scupmt,\n" + 
           "    'JDE' as username, 'OPEN' as account_status " +
           "from "+library + ".f98owsec\n" + 
           "where scupmj > ?\n" + 
           "or ( scupmj = ? and scupmt > ? )";

       
        PreparedStatement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
       
        try {
                stmt = connection.prepareStatement(query[dbtype]);
                //stmt.setObject(1, activity_date);
                //stmt.setObject(2, activity_date);
                //stmt.setObject(3, activity_time);

                ResultSet rs = stmt.executeQuery();
              
                while (rs.next()) {
                    System.out.println(
                    "proxyuser=" + rs.getString("USERNAME") +
                    ",proxyenabled=" + rs.getString("ACCOUNT_STATUS") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        // setLastProcRun("DISABLED_USER");
    }
    
    public void getOmwObjects() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
           "SELECT\n" + 
           "    poomwprjid as project_name,\n" + 
           "    poomwobjid as object_name,\n" + 
           "    poomwot as object_type,\n" + 
           "    poomwuser as project_user,\n" + 
           "    popathcd as path_code,\n" + 
           "    poomwchs as status,\n" + 
           "    poupmj, poupmt\n" + 
           "from " + library + "f98222\n" + 
           "where \n" + 
           "   ( poupmj > %d\n" + 
           "or ( poupmj = %d and poupmt > %d ))";

       
        String query = null;
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("omw_objects");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "project_name=" + rs.getString("PROJECT_NAME").trim() +
                    ",object_name=" + rs.getString("OBJECT_NAME").trim() +
                    ",object_type=" + rs.getString("OBJECT_TYPE").trim() +
                    ",project_user=" + rs.getString("PROJECT_USER").trim() +
                    ",path_code=" + rs.getString("PATH_CODE").trim() +
                    ",status=" + rs.getString("STATUS") +
                    ",change_date=" + time_fmt.format( from_jde_date( rs.getInt("POUPMJ"), rs.getInt("POUPMT") )) 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("omw_objects");
    }
    
    public void getOmwVersions() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        
        String null_value = "nvl";
        String concat = "||";
        
        if (dbtype == 1) { 
            null_value = "isnull";
            concat = "+";
        }
        if (dbtype == 2) {
            null_value = "ifnull";
        }
        
        String query_pattern =
           "SELECT ProjectID, \n" + 
           "                ProjDesc, ObjectType,\n" + 
           "                Objects,\n" + 
           "                " + null_value + "(rtrim(vrvers),'null') AS Version,\n" + 
           "                " + null_value + "(ObjectDesc,rTRIM(VrJD)) AS OBJDesc,\n" + 
           "                ObjScope,\n" + 
           "                CreatedDate,\n" + 
           "                ModifiedDate, ModifiedTime\n" + 
           " FROM (\n" + 
           "                SELECT rtrim(pmomwprjid) AS ProjectID,\n" + 
           "                                rtrim(pmomwdesc) AS ProjDesc,\n" + 
           "                                rtrim(poomwobjid) AS Objects,\n" + 
           "                                poomwot AS ObjectType,\n" + 
           "                                rtrim(simd) AS ObjectDesc,\n" + 
           "                                CASE\n" + 
           "                                                WHEN poomwcrtdt = 0 THEN poupmj\n" + 
           "                                                WHEN poomwcrtdt > 0 THEN poomwcrtdt\n" + 
           "                                END AS CreatedDate,\n" + 
           "                    CASE\n" + 
           "                      WHEN poomwcrtdt=0 THEN 'MOD'\n" + 
           "                      WHEN poomwcrtdt>0 THEN 'NEW'\n" + 
           "                    END AS ObjScope,\n" + 
           "                    poupmj AS ModifiedDate,\n" + 
           "                    poupmt AS ModifiedTime\n" + 
           "                  FROM " + library + "F98220 A\n" + 
           "                   INNER JOIN " + library + "F98222 B\n" + 
           "                    ON A.pmomwprjid = B.poomwprjid\n" + 
           "                  LEFT OUTER JOIN " + ollib + "F9860 C\n" + 
           "                    ON B.poomwobjid = C.siobnm\n" + 
           ") X \n" + 
           "left outer JOIN " + colib + "F983051\n" + 
           "ON (rtrim(Objects) = (rtrim(vrpid)\n" + 
           "  " + concat + " '|'\n" + 
           "  " + concat + " rtrim(vrvers)))\n" + 
           "where\n" + 
           "   ( ModifiedDate > %d\n" + 
           "or ( ModifiedDate = %d and ModifiedTime > %d ))\n";

       
        String query = null;
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("omw_versions");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
        // System.out.println(query);
        
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "project_id=" + rs.getString("PROJECTID").trim() +
                    ",project_desc=" + rs.getString("PROJDESC").trim() +
                    ",object_type=" + rs.getString("OBJECTTYPE").trim() +
                    ",objects=" + rs.getString("OBJECTS").trim() +
                    ",version=" + rs.getString("VERSION").trim() +
                    ",obj_desc=" + rs.getString("OBJDESC") +
                    ",obj_scope=" + rs.getString("OBJSCOPE") +
                    ",created=" + time_fmt.format( from_jde_date( rs.getInt("CREATEDDATE"), 0 )) + 
                    ",modified=" + time_fmt.format( from_jde_date( rs.getInt("MODIFIEDDATE"), rs.getInt("MODIFIEDTIME") )) 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("omw_versions");
    }

    public void getOmwProjects() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
           "SELECT\n" + 
           " pmomwprjid as project_name,\n" + 
           " pmomwdesc as description,\n" + 
           " pmomwps project_status,\n" + 
           " pmupmj, pmupmt \n" + 
           "from "+ library + "f98220\n" + 
           "where  \n" + 
           "    ( pmupmj > %d\n" + 
           " or ( pmupmj = %d and pmupmt > %d ))";

       
        String query = null;
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("omw_projects");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
       
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "project_name=" + rs.getString("PROJECT_name").trim() +
                    ",description=" + rs.getString("DESCRIPTION").trim() +
                    ",project_status=" + rs.getString("PROJECT_STATUS").trim() +
                    ",change_date=" + time_fmt.format( from_jde_date( rs.getInt("PMUPMJ"), rs.getInt("PMUPMT") )) 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("omw_projects");
    }
    
    public void getSalesOrderLeftBehind() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query_pattern =
           " SELECT\n" + 
           "     SDDOCO,\n" + 
           "     SDDCTO,\n" + 
           "     SDKCOO,\n" + 
           "     SDADDJ,\n" + 
           "     SDLITM,\n" + 
           "     SDIVD,\n" + 
           "     SDNXTR,\n" + 
           "     SDLTTR,\n" + 
           "     SDDSC1\n" + 
           " FROM " + prodlib +  "f4211 \n" + 
           " WHERE sdnxtr = 999\n" + 
           "  AND sdlttr = 620\n" + 
           "  and SDADDJ > %d";

       
        String query = null;
        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
        Timestamp lastJobRun = getLastProcRun("sales_order_left_behind");
        Integer iLastRunDate = to_jde_date(lastJobRun);
        Integer iLastRunTime = to_jde_time(lastJobRun);
       
        query = String.format(query_pattern, iLastRunDate, iLastRunDate, iLastRunTime);
        
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    time_fmt.format(current_date) +
                    "document=" + rs.getString("SDDOCO").trim() +
                    ",order_type=" + rs.getString("SDDCTO") +
                    ",order_company=" + rs.getString("SDKCOO") +
                    ",ship_date=" + time_fmt.format( from_jde_date( rs.getInt("SDADDJ"), 0 )) +
                    ",item_number=" + rs.getString("SDLITM") +
                    ",invoice_date=" + rs.getString("SDIVD") +
                    ",status_code_next=" + rs.getString("SDNXTR") +
                    ",status_code_last=" + rs.getString("SDLTTR") +
                    ",desc=" + rs.getString("SDDSC1") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("sales_order_left_behind");
    }
    
    public void getSqlSpace() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query1 = 
        "Create Table ##temp\n" + 
        "(\n" + 
        "    DatabaseName sysname,\n" + 
        "    Name sysname,\n" + 
        "    physical_name nvarchar(500),\n" + 
        "    size decimal (18,2),\n" + 
        "    FreeSpace decimal (18,2)\n" + 
        ")  \n" + 
        "Exec sp_msforeachdb '\n" + 
        "Use [?];\n" + 
        "Insert Into ##temp (DatabaseName, Name, physical_name, Size, FreeSpace)\n" + 
        "    Select DB_NAME() AS [DatabaseName], Name,  physical_name,\n" + 
        "    Cast(Cast(Round(cast(size as decimal) * 8.0/1024.0,2) as decimal(18,2)) as nvarchar) Size,\n" + 
        "    Cast(Cast(Round(cast(size as decimal) * 8.0/1024.0,2) as decimal(18,2)) -\n" + 
        "        Cast(FILEPROPERTY(name, ''SpaceUsed'') * 8.0/1024.0 as decimal(18,2)) as nvarchar) As FreeSpace\n" + 
        "    From sys.database_files\n" + 
        "' \n" ;
        String drop = "drop table ##temp";
        String query  = 
        "Select\n" + 
        "                databasename,\n" + 
        "                round((sum( size ) - sum(freespace))*100,1) / sum(size) as used_percent,\n" + 
        "    sum( size ) - sum( freespace ) used_space,\n" + 
        "    sum( size ) as container_size,\n" + 
        "    sum( freespace ) as free_space\n" + 
        "From ##temp\n" + 
        "group by databasename";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
              
        try {
                stmt = connection.createStatement();
                stmt.executeUpdate(query1);
                ResultSet rs = stmt.executeQuery(query);
              
                  while (rs.next()) {
                      System.out.println(
                      "databasename=" + rs.getString("DATABASENAME") +
                      ",used_percent=" + rs.getString("USED_PERCENT").trim() +
                      ",used_space=" + rs.getString("USED_SPACE").trim() +
                      ",free_space=" + rs.getString("FREE_SPACE").trim() +
                      ",container_size=" + rs.getString("CONTAINER_SIZE")
                       );
                  }
                stmt.executeUpdate(drop);
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
    

    public void getSqlLogins() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = "exec sp_who";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
              
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "loginame=" + rs.getString("LOGINAME") +
                    ",status=" + rs.getString("STATUS").trim() +
                    ",hostname=" + rs.getString("HOSTNAME").trim() +
                    ",dbname=" + rs.getString("DBNAME")
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
            

    public void getSqlTps() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Long cntr_value = 0L;
        
        String query = "SELECT  sum(cntr_value) as cntr_value\n" + 
        "    FROM sys.dm_os_performance_counters\n" + 
        "    WHERE counter_name = 'transactions/sec'\n" + 
        "        AND object_name = 'SQLServer:Databases'\n" + 
        "        AND instance_name like 'JDE%' " ;

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
       
              
        try {
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            cntr_value = rs.getLong("CNTR_VALUE");
            Thread.sleep(60000);
            rs = stmt.executeQuery(query);
            rs.next();
            System.out.println( time_fmt.format(current_date) + " tps=" + (rs.getLong("CNTR_VALUE")-cntr_value)/60 );
            
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
    }
            
    public void getSqlBackups() {
        SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        SimpleDateFormat  time_fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String query_pattern = "SELECT\n" + 
        "   CONVERT(CHAR(100), SERVERPROPERTY('Servername')) AS Server,\n" + 
        "   msdb.dbo.backupset.database_name,\n" + 
        "   cast(MAX(msdb.dbo.backupset.backup_finish_date) as varchar) AS last_db_backup_date\n" + 
        "FROM   msdb.dbo.backupmediafamily\n" + 
        "   INNER JOIN msdb.dbo.backupset ON msdb.dbo.backupmediafamily.media_set_id = msdb.dbo.backupset.media_set_id\n" + 
        "WHERE  msdb..backupset.type = 'D'\n" + 
        "AND msdb.dbo.backupset.backup_finish_date > convert(datetime, '%s', 20)\n" + 
        "GROUP BY\n" + 
        "   msdb.dbo.backupset.database_name\n" + 
        "ORDER BY\n" + 
        "   msdb.dbo.backupset.database_name";

        Statement stmt = null;
        
        Integer date_t;
        java.util.Date cdate = new java.util.Date();
        Date current_date = new Date( cdate.getTime());
        
        Timestamp lastJobRun = getLastProcRun("backup_report");
        
        String query = String.format(query_pattern, time_fmt.format(lastJobRun));
        // System.out.println(query);      
        try {
                stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "server=" + rs.getString("SERVER").trim() +
                    ",database_name=" + rs.getString("DATABASE_NAME").trim() +
                    ",last_db_backup_date=" + rs.getString("LAST_DB_BACKUP_DATE").trim()
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }
        setLastProcRun("backup_report");

    }
            
    public void getSqlRowCount() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        // SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
        "SELECT top 30  T.name TableName,i.Rows NumberOfRows\n" + 
        "FROM        sys.tables T\n" + 
        "JOIN        sys.sysindexes I ON T.OBJECT_ID = I.ID\n" + 
        "WHERE       indid IN (0,1)\n" + 
        "ORDER BY    i.Rows DESC,T.name";

        Statement stmt = null;
        
        // Integer date_t;
        // java.util.Date cdate = new java.util.Date();
        // Date current_date = new Date( cdate.getTime());
        
              
        try {
                stmt = connection.createStatement();
                stmt.execute("use jde_production");
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "tablename=" + rs.getString("TABLENAME").trim() +
                    ",numberofrows=" + rs.getString("NUMBEROFROWS").trim() 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }

    }
    
    public void getSqlPageLifeExpectancy() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        // SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
        "SELECT cntr_value AS ple\n" + 
        "FROM sys.dm_os_performance_counters\n" + 
        "WHERE OBJECT_NAME = N'SQLServer:Buffer Manager' \n" + 
        "AND counter_name = N'Page life expectancy'";

        Statement stmt = null;
                
              
        try {
                stmt = connection.createStatement();
                // stmt.execute("use jde_production");
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "server=" + host + 
                    ",ple=" + rs.getInt("PLE") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }

    }

        
    public void getSqlDBBufferUsage() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        // SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
        "SELECT DB_NAME(database_id) AS [DBName],\n" + 
        "round(COUNT(*) * 8/1024.0,0) AS [CachedSize]\n" + 
        "FROM sys.dm_os_buffer_descriptors\n" + 
        "WHERE database_id > 4 -- system databases\n" + 
        "AND database_id <> 32767 -- ResourceDB\n" + 
        "GROUP BY DB_NAME(database_id)\n" + 
        "having COUNT(*) * 8/1024.0 > 1"; 

        Statement stmt = null;
                
              
        try {
                stmt = connection.createStatement();
                // stmt.execute("use jde_production");
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "server=" + host + 
                    ",db_name=" + rs.getString("DBNAME") +
                    ",cached_size=" + rs.getInt("CACHEDSIZE") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }

    }

    public void getSqlSysMemory() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        // SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        String query = 
        "SELECT total_physical_memory_kb, available_physical_memory_kb, \n" + 
        "       total_page_file_kb, available_page_file_kb \n" + 
        "FROM sys.dm_os_sys_memory";

        Statement stmt = null;
                
              
        try {
                stmt = connection.createStatement();
                // stmt.execute("use jde_production");
                ResultSet rs = stmt.executeQuery(query);
              
                while (rs.next()) {
                    System.out.println(
                    "server=" + host + 
                    ",total_physical_memory_kb=" + rs.getLong("TOTAL_PHYSICAL_MEMORY_KB") +
                    ",available_physical_memory_kb=" + rs.getLong("AVAILABLE_PHYSICAL_MEMORY_KB") +
                    ",total_page_file_kb=" + rs.getLong("TOTAL_PAGE_FILE_KB") +
                    ",available_page_file_kb=" + rs.getLong("AVAILABLE_PAGE_FILE_KB") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }

    }

    public void getSqlProcAddrSpace() {
        // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy");
        // SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        Long pageFaultCnt = 0L;
        
        String query =             
        "SELECT physical_memory_in_use_kb,locked_page_allocations_kb, \n" + 
        "       page_fault_count, memory_utilization_percentage, \n" + 
        "       available_commit_limit_kb, process_physical_memory_low, \n" + 
        "       process_virtual_memory_low\n" + 
        "FROM sys.dm_os_process_memory";

        Statement stmt = null;
                
              
        try {
                stmt = connection.createStatement();
                // stmt.execute("use jde_production");
                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                pageFaultCnt = rs.getLong("PAGE_FAULT_COUNT");
            
                Thread.sleep(10000);
                rs = stmt.executeQuery(query);
          
              
                while (rs.next()) {
                    System.out.println(
                    "server=" + host + 
                    ",physical_memory_in_use_kb=" + rs.getLong("PHYSICAL_MEMORY_IN_USE_KB") + 
                    ",locked_page_allocations_kb=" + rs.getLong("LOCKED_PAGE_ALLOCATIONS_KB") + 
                    ",page_fault_count=" + (rs.getLong("PAGE_FAULT_COUNT") - pageFaultCnt)/10 + 
                    ",memory_utilization_percentage=" + rs.getInt("MEMORY_UTILIZATION_PERCENTAGE") + 
                    ",available_commit_limit_kb=" + rs.getLong("AVAILABLE_COMMIT_LIMIT_KB") + 
                    ",process_physical_memory_low=" + rs.getInt("PROCESS_PHYSICAL_MEMORY_LOW") + 
                    ",process_virtual_memory_low=" + rs.getInt("PROCESS_VIRTUAL_MEMORY_LOW") 
                     );
                }
            } catch (SQLException e ) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
        }

    }

    public void iSeriesRunUbe() {
        
        String report = ini.get(connName, "report");
        String version = ini.get(connName, "version");
        String e1user = ini.get(connName, "e1user");
        String e1pass = ini.get(connName, "e1pass");
        String environ = ini.get(connName, "environ");
        
        String ubecmd = String.format(
        "SBMJOB CMD(RUNUBE USER(%s) PASSWORD(%s) ENVIRON(%s) REPORT(%s) VERSION(%s) JOBQ(QBATCH) IORB(B))",
            e1user, e1pass, environ, report, version );

        iSeriesCmd(ubecmd);
        
    }

    @SuppressWarnings("oracle.jdeveloper.java.insufficient-catch-block")
    public void iSeriesProgram() {
        AS400 as400 = null;
        // Create an AS400 object  
        as400 = new AS400(host, userId, password);  
        String commandStr = "E900SYS/RUNUBE";
        String env = library;
        String ube = "R0006P";
        String version = "XJDE0001";
        String qbatch = "QBATCH";
        String batch = "BATCH";
        String hold = "HOLD";
        String save = "SAVE";
        String returnMsg; //return value
         
        // Create a Command object
        ProgramCall program = new ProgramCall(as400);

        try  {
            ProgramParameter[] plist = new ProgramParameter[10];
            AS400Text textData = new AS400Text(20, as400);

            plist[0] = new ProgramParameter(textData.toBytes(userId));
              textData = new AS400Text(20, as400);
            plist[1] = new ProgramParameter(textData.toBytes(password));
              textData = new AS400Text(7, as400);
            plist[2] = new ProgramParameter(textData.toBytes(env));
              textData = new AS400Text(6, as400);
            plist[3] = new ProgramParameter(textData.toBytes(ube));
              textData = new AS400Text(8, as400);
            plist[4] = new ProgramParameter(textData.toBytes(version)); 
              textData = new AS400Text(6, as400);
            plist[5] = new ProgramParameter(textData.toBytes(qbatch));  
              textData = new AS400Text(5, as400);
            plist[6] = new ProgramParameter(textData.toBytes(batch)); 
              textData = new AS400Text(4, as400);
            plist[7] = new ProgramParameter(textData.toBytes(hold)); 
              textData = new AS400Text(4, as400);
            plist[8] = new ProgramParameter(textData.toBytes(save)); 
            plist[9] = new ProgramParameter(100);                                 
        
           // Run the command.
           // System.out.println("Executing: " + commandStr);
            program.setProgram(commandStr, plist);
            
        
           if (program.run() != true) { 
               System.out.println("Command Failed!");
               AS400Message[] messageList = program.getMessageList();
               for (AS400Message message : messageList){
                   System.out.println(message.getText());
               }
           } else {
               System.out.println("Command Executed Successfully.");    
               textData = new AS400Text(100, as400);
               returnMsg = (String) textData.toObject(plist[9].getOutputData());
           }
            
           // Get the command results
          } catch (Exception e) {  
           e.printStackTrace();  
          } finally{
           try{
            // Make sure to disconnect   
            as400.disconnectAllServices();  
           } catch(Exception e){}
        }  
    }

    /**
     * @param commandStr
     */
    public void iSeriesCmd(String commandStr) {
        AS400 as400 = null;
        try  {
           // Create an AS400 object  
           as400 = new AS400(host, userId, password);  
            
           // Create a Command object
           CommandCall command = new CommandCall(as400);
         
           // Run the command.
           // System.out.println("Executing: " + commandStr);
           boolean success = command.run(commandStr);
            
           if (success) {  
            // System.out.println("Command Executed Successfully.");
           }else{
            System.out.println("Command Failed!");
           }
            
           // Get the command results
           AS400Message[] messageList = command.getMessageList();
           for (AS400Message message : messageList){
            System.out.println(message.getText());
           }
          } catch (Exception e) {  
           e.printStackTrace();  
          } finally{
           try{
            // Make sure to disconnect   
            as400.disconnectAllServices();  
           } catch(Exception e){}
        }  
    }
    
    public void getQMsg() {
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        AS400 system = new AS400(host, userId, password);
        
        try { 
            MessageQueue queue = new MessageQueue(system,"/QSYS.LIB/QSYSOPR.MSGQ" ); 
            Timestamp lastJobRun = getLastProcRun("qsysopr");
            Enumeration e = queue.getMessages(); 
        
            while (e.hasMoreElements()) { 
               QueuedMessage message = (QueuedMessage) e.nextElement();
               // System.out.println(message.getText());
               Calendar c = message.getDate();
               java.util.Date d = c.getTime();
               
               if (d.compareTo(lastJobRun) > 0) {
                   System.out.println(
                   time_fmt.format(d) + 
                   ",msg_id=" + message.getID() + 
                   ",severity=" + message.getSeverity() + 
                   ",msg_text=\"" + message.getText() +
                   "\",msg_type=" + message.getType() + 
                   ",job_name=" + message.getFromJobName() + 
                   ",job_user=" + message.getUser() + 
                   ",job_number=" + message.getFromJobNumber() + 
                   ",program=" + message.getFromProgram() + 
                   ",cur_user=" + message.getCurrentUser() );
               }
               
               // 1 = completion   4 = info , 5 = inquiry
               // "MsgDate=" + time_fmt.format(d) + ",lastRunDate=" + time_fmt.format(lastJobRun) + ",compare=" + d.compareTo(lastJobRun)  );
               
               
                // System.out.println(e.nextElement()); 
            } 
        } catch (Exception e) { e.printStackTrace(); }
        setLastProcRun("qsysopr");
    }
    
    public void getMsg() {
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        AS400 system = new AS400(host, userId, password);
        
        try { 
            MessageQueue queue = new MessageQueue(system,"/BRMS.LIB/BRMSUSER.MSGQ" ); 
            // Timestamp lastJobRun = getLastProcRun("QSYSOPR");
            Enumeration e = queue.getMessages(); 
        
            while (e.hasMoreElements()) { 
               QueuedMessage message = (QueuedMessage) e.nextElement();
               // System.out.println(message.getText());
               Calendar c = message.getDate();
               java.util.Date d = c.getTime();
               
               if (1 > 0) {
                   System.out.println(
                   time_fmt.format(d) + 
                   ",msg_id=" + message.getID() + 
                   ",severity=" + message.getSeverity() + 
                   ",msg_text=\"" + message.getText() +
                   "\",msg_type=" + message.getType() + 
                   ",job_name=" + message.getFromJobName() + 
                   ",job_user=" + message.getUser() + 
                   ",job_number=" + message.getFromJobNumber() + 
                   ",program=" + message.getFromProgram() + 
                   ",cur_user=" + message.getCurrentUser() );
               }
               
               // 1 = completion   4 = info , 5 = inquiry
               // "MsgDate=" + time_fmt.format(d) + ",lastRunDate=" + time_fmt.format(lastJobRun) + ",compare=" + d.compareTo(lastJobRun)  );
               
               
                // System.out.println(e.nextElement()); 
            } 
        } catch (Exception e) { e.printStackTrace(); }
      //  setLastProcRun("QSYSOPR");
    }
    
    public void getAS400Info() {
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        AS400 system = new AS400(host, userId, password);
        SystemStatus ss = new SystemStatus(system);
        SystemPool sp ;
        java.util.Date d = new java.util.Date();

        try {
            ss.setCaching(true);
            
            for (@SuppressWarnings("unchecked")
                 Enumeration<SystemPool> e = ss.getSystemPools(); e.hasMoreElements() ;) {
                sp = e.nextElement();
                System.out.println( 
                    time_fmt.format(d) +
                    ",pool=" + sp.getName() + 
                    ",size=" + sp.getSize() +
                    ",reserve=" + sp.getReservedSize() +  
                    ",dbpages=" + sp.getDatabasePages() + 
                    ",dbfaults=" + sp.getDatabaseFaults() + 
                    ",nondbpages=" + sp.getNonDatabasePages() +
                    ",nondbfaults=" + sp.getNonDatabaseFaults() 
                    );
            }
                
            System.out.println( 
                time_fmt.format(d) +
                ",cpucap=" + ss.getCurrentProcessingCapacity() + 
                ",cpuuncap=" + ss.getPercentUncappedCPUCapacityUsed() +
                ",cpupct=" + ss.getPercentProcessingUnitUsed() +
                ",cpucnt=" + ss.getNumberOfProcessors() +
                ",totaljobs=" + ss.getJobsInSystem() + 
                ",activejobs=" + ss.getActiveJobsInSystem() +
                ",batchjobs=" + ss.getBatchJobsRunning() +
                ",jobswaiting4msg=" + ss.getBatchJobsWaitingForMessage() +
                ",jobsheld=" + ss.getBatchJobsHeldOnJobQueue() +
                ",diskspacetotal=" + ss.getSystemASP() / 1000  + 
                ",diskspaceavail=" + ss.getSystemASP() * ((100 - ss.getPercentSystemASPUsed())/100) / 1000 + 
                ",diskusedpct=" + ss.getPercentSystemASPUsed() +
                ",interactive=" + ss.getPercentCurrentInteractivePerformance() + 
                ",usercnt=" + ss.getUsersCurrentSignedOn()
            ); // workwithdiskstats
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


// display ptf status - release version, ptf applied (patch) product id is machine 
// work ptf group patch levels
// display software resources
    
// index advisor    
    
    
    @SuppressWarnings("oracle.jdeveloper.java.semantic-warning")
    public void getIseriesJobInfo() {
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        AS400 system = new AS400(host, userId, password);
        java.util.Date d = new java.util.Date();

        try {
            JobList jobs = new JobList(system);
            
            Enumeration jlist = jobs.getJobs();
            
            while (jlist.hasMoreElements()) {
                Job job = (Job) jlist.nextElement();
                if (job.getStatus().equals("*ACTIVE") && job.getCPUUsed() > 0) {
                    
                    System.out.println( "activeDate="+ job.getJobActiveDate() + 
                                        ",jobNumber=" + job.getNumber() + 
                                        ",jobName=" + job.getName() +
                                        ",jobUser=" + job.getUser() + 
                                        ",jobCpu=" + job.getCPUUsed() +
                                        ",IOrequests=" + job.getAuxiliaryIORequests() +
                                        ",defaultWait=" + job.getDefaultWait());
                    
                    /*
                    JobLog jlog = new JobLog(system, job.getName(), job.getUser(), job.getNumber());
                    
                    Enumeration messageList = jlog.getMessages();
                    
                    while (messageList.hasMoreElements()) {
                        AS400Message message = (AS400Message) messageList.nextElement();
                        System.out.println( job.getName() + ",msg_txt=\"" +  message.getText() + "\""   );
                    }
                   */
                }
            }
            
             
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    public void getJobLog() {
        SimpleDateFormat  time_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
        AS400 system = new AS400(host, userId, password);
        java.util.Date d = new java.util.Date();

        try {
            JobLog jlog = new JobLog(system);
            jlog.setName("SAVESYSTEM");
            jlog.setUser("BRMUSER");
            
            Enumeration messageList = jlog.getMessages();
            
            while (messageList.hasMoreElements()) {
                AS400Message message = (AS400Message) messageList.nextElement();
                System.out.println( message.getDate() + " " + message.getText() );
            }
             
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    public void testdate() {
       // java.util.Date cdate = new java.util.Date();
       // Date current_date = new Date( cdate.getTime());
       // SimpleDateFormat  date_fmt = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
       // Timestamp lastJobRun = getLastProcRun("SUBMITTED_JOBS");
       // Integer iLastRunDate = to_jde_date(lastJobRun);
       // Integer iLastRunTime = to_jde_time(lastJobRun);
       // System.out.println(date_fmt.format( from_jde_date( 114315, 0 )));
    }

    @SuppressWarnings("oracle.jdeveloper.java.tag-is-missing")
    public static void main(String[] args) {
        if (args.length == 0) System.exit(1);
        
        String connName = "jdeprod";
        if (args.length >= 2) connName = args[1];

        pulsar i = new pulsar(connName);    
        //System.out.println(i.getLastProcRun("submitted_jobs"));
        //i.setLastProcRun("submitted_jobs");
        
        //System.exit(0);
        
        switch ( args[0] ) {
        case "1" : 
        case "getsubmittedjobs" :
            i.getSubmittedJobs();
            break;
        
        case "2" :
        case "getworkcentermessages" :
            i.getWorkCenterMessages();
            break;
        
        case "3" :
        case "getpackagebuilds" :
            i.getPackageBuilds();
            break;
                
        case "4" :
        case "getqsysopermessages" :
            i.getQMsg();
            break;
        
        case "5" :
        case "getoraclelastbackup" :
            i.getOracleLastBackup();
            break;
 
        case "6" :
        case "getoraclebackupreport" :
            i.getOracleBackupReport();
            break;
        
        case "7" :
        case "getoracledbfilestat" :
            i.getOracleDBFileStat();
            break;
        
        case "8" :
        case "getoracledbsegmentstat" :
            i.getOracleDBSegmentStat();
            break;
        
        case "9" :
        case "getoracledbiostat" :
            i.getOracleDBIOStat();
            break;
        
        case "10" :
        case "getoraclesql" :
            i.getOracleSql();
            break;
        
        case "11" :
        case "getoraclesessions" :
            i.getOracleSessions();
            break;
        
        case "12" :
        case "getoracletablespace" :
            i.getOracleTablespace();
            break;
        
        case "13" :
        case "getoracletps" :
            i.getOracleTps();
            break;
        
        case "14" :
        case "getoracleusercalls" :
            i.getOracleUserCalls();
            break;
        
        case "15" :
        case "getoraclewaitevent" :
            i.getOracleWaitEvent();
            break;
        
        case "16" :
        case "getoraclerowcount" :
            i.getOracleRowCount();
            break;

        case "17" :
        case "getoraclelocks" :
            i.getOracleLocks();
            break;
        
        case "18" :
        case "getsecurityhistory" :
            i.getSecurityHistory();
            break;

        case "19" :
        case "getdisableduser" :
            i.getDisabledUser();
            break;
        
        case "20" :
        case "getomwobjects" :
            i.getOmwObjects();
            break;
        
        case "21" :
        case "getomwversions" :
            i.getOmwVersions();
            break;
        
        case "22" :
        case "getomwprojects" :
            i.getOmwProjects();
            break;
        
        case "23" :
        case "getsalesorderleftbehind" :
         i.getSalesOrderLeftBehind();
            break;

        case "24" :
        case "getsqllogins" :
            i.getSqlLogins();
            break;
        
        case "25" :
        case "getsqlspace" :
            i.getSqlSpace();
            break;
        
        case "26" :
        case "getsqltps" :
            i.getSqlTps();
            break;
        
        case "27" :
        case "getsqlbackups" :
            i.getSqlBackups();
            break;
            
        case "28" :
        case "getsqlrowcount" :
            i.getSqlRowCount();
            break;

        case "29" :
        case "getSqlPageLifeExpectancy" :
            i.getSqlPageLifeExpectancy();
            break;

        case "30":
        case "getSqlDBBufferUsage" :
            i.getSqlDBBufferUsage();
            break;

        case "31":
        case "getSqlSysMemory" :
            i.getSqlSysMemory();
            break;

        case "32" :
        case "getSqlProcAddrSpace" :
            i.getSqlProcAddrSpace();
            break;

        case "33" :
        case "getoracledbinfo" :
            i.getOracleDbInfo();
            break;
        
        case "34" :
        case "getsubsystemjobs" :
            i.getSubSystemJobs();
            break;
        
        case "35" :
        case "getproxyuser" :
            i.getProxyUser();
            break;
        
        case "36" :
        case "getschedulerinfo" :
            i.getSchedulerInfo();
            break;
            
        case "37" :
        case "getwsjcount" :
            i.getWsjCount();
            break;
       
        case "38" :
        case "getSysOprMsg" :
            i.getQMsg();
            break;
        
        case "39" :
        case "getas400info" :
            i.getAS400Info();
            break;
        
        case "40" :
        case "runube" :
            i.iSeriesRunUbe();
            break;
        
        case "41":
        case "getjobs" :
            i.getIseriesJobInfo();
            break;

        case "42":
        case "gettablerowcnt" :
            String tables[] = new String[args.length - 2];
            System.arraycopy( args, 2, tables, 0, args.length -2 );
            i.getTableRowCnt(tables);
            break;
                
        case "43" :
        case "getMsg" :
            i.getMsg();
            break;
            
                      
        }
               
    }
}
