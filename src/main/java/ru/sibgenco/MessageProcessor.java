package ru.sibgenco;

import java.lang.Math;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import jdk.internal.jshell.tool.resources.l10n;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;

import java.sql.*;

public class MessageProcessor implements Runnable {
    public MessageProcessor(String chConnectionString, String backendTableName){
        this.chConnectionString = chConnectionString;
        this.backendTableName = backendTableName;
    }
    private String chConnectionString;
    private String backendTableName; 
    
    private final static String TABLE_CREATION_QUERY =       
        "CREATE TABLE IF NOT EXISTS   {0} (                                                                               \n" +
        "  timestamp  DateTime(''Asia/Krasnoyarsk'') NOT NULL, -- Epoch Unix timestamp (See https://www.unixtimestamp.com)  \n" +
        "  id               INTEGER                NOT NULL, -- District heating network object id                        \n" +
        "  tit01heatt2      INTEGER                NOT NULL, -- Temperature of return heating water, [°C]×100             \n" +
        "  tit02netwt2      INTEGER                NOT NULL, -- Temperature of return network water, [°C]×100             \n" +
        "  tit03hotwt1      INTEGER                NOT NULL, -- Temperature of hot water, [°C]×100                        \n" +
        "  tit04heatt1      INTEGER                NOT NULL, -- Temperature of supply heating water, [°C]×100             \n" +
        "  tit05netwt1      INTEGER                NOT NULL, -- Temperature of supply network water, [°C]×100             \n" +
        "  tit06utilp1      INTEGER                NOT NULL, -- Pressure of water utility, [kg×f/cm²]×100                 \n" +
        "  tit07heatp2      INTEGER                NOT NULL, -- Pressure of return heating water, [kg×f/cm²]×100          \n" +
        "  tit08hotwp1      INTEGER                NOT NULL, -- Pressure of hot water, [kg×f/cm²]×100                     \n" +
        "  tit09heatp1      INTEGER                NOT NULL, -- Pressure of supply heating water, [kg×f/cm²]×100          \n" +
        "  tit10netwp2      INTEGER                NOT NULL, -- Pressure of return network water, [kg×f/cm²]×100          \n" +
        "  tit11netwp1      INTEGER                NOT NULL, -- Pressure of supply network water, [kg×f/cm²]×100          \n" +
        
        "  ts001prelay      TINYINT                NOT NULL, -- Plugin: relay of the drainage pit                         \n" +
        "  ts002p1hotf      TINYINT                NOT NULL, -- Failure: hot water pump-1                                 \n" +
        "  ts003p2hotf      TINYINT                NOT NULL, -- Failure: hot water pump-2                                 \n" +
        "  ts004p3hotf      TINYINT                NOT NULL, -- Failure: hot water pump-3                                 \n" +
        "  ts005p1heaf      TINYINT                NOT NULL, -- Failure: heating pump-1                                   \n" +
        "  ts006p2heaf      TINYINT                NOT NULL, -- Failure: heating pump-2                                   \n" +
        "  ts007p3heaf      TINYINT                NOT NULL, -- Failure: heating pump-3                                   \n" +
        "  ts008p1recf      TINYINT                NOT NULL, -- Failure: recharge pump-1                                  \n" +
        "  ts009p2recf      TINYINT                NOT NULL, -- Failure: recharge pump-2                                  \n" +
        "  ts010p1cirf      TINYINT                NOT NULL, -- Failure: circulation pump-1                               \n" +
        "  ts011p2cirf      TINYINT                NOT NULL, -- Failure: circulation pump-2                               \n" +
        "  ts012drains      TINYINT                NOT NULL, -- Plugin: drainage pump                                     \n" +
        "  ts013p1hots      TINYINT                NOT NULL, -- Plugin: hot water pump-1                                  \n" +
        "  ts014p2hots      TINYINT                NOT NULL, -- Plugin: hot water pump-2                                  \n" +
        "  ts015p3hots      TINYINT                NOT NULL, -- Plugin: hot water pump-3                                  \n" +
        "  ts016p1heas      TINYINT                NOT NULL, -- Plugin: heating pump-1                                    \n" +
        "  ts017p2heas      TINYINT                NOT NULL, -- Plugin: heating pump-2                                    \n" +
        "  ts018p3heas      TINYINT                NOT NULL, -- Plugin: heating pump-3                                    \n" +
        "  ts019p1recs      TINYINT                NOT NULL, -- Plugin: recharge pump-1                                   \n" +
        "  ts020p2recs      TINYINT                NOT NULL, -- Plugin: recharge pump-2                                   \n" +
        "  ts021p1cirs      TINYINT                NOT NULL, -- Plugin: circulation pump-1                                \n" +
        "  ts022p2cirs      TINYINT                NOT NULL, -- Plugin: circulation pump-2                                \n" +
        "  ts023alarms      TINYINT                NOT NULL, -- Plugin: security alarm system                             \n" +
        "  ts024rchots      TINYINT                NOT NULL, -- Plugin: remote control of hot water                       \n" +
        "  ts025rcheas      TINYINT                NOT NULL, -- Plugin: remote control of heating                         \n" +
        "  ts026rcrecs      TINYINT                NOT NULL, -- Plugin: remote control of recharge                        \n" +
        "  ts027rccirs      TINYINT                NOT NULL, -- Plugin: remote control of circulation                     \n" +
        "  ts028auhots      TINYINT                NOT NULL, -- Plugin: hot water automation                              \n" +
        "  ts029auheas      TINYINT                NOT NULL, -- Plugin: heating automation                                \n" +
        "  ts030aurecs      TINYINT                NOT NULL, -- Plugin: recharge automation                               \n" +
        "  ts031aucirs      TINYINT                NOT NULL, -- Plugin: circulation automation                            \n" +
        "  ts032pwmons      TINYINT                NOT NULL, -- Plugin: power supply monitoring                           \n" +
        "  ts033tit01f      TINYINT                NOT NULL, -- Failure: TIT01                                            \n" +
        "  ts034tit02f      TINYINT                NOT NULL, -- Failure: TIT02                                            \n" +
        "  ts035tit03f      TINYINT                NOT NULL, -- Failure: TIT03                                            \n" +
        "  ts036tit04f      TINYINT                NOT NULL, -- Failure: TIT04                                            \n" +
        "  ts037tit05f      TINYINT                NOT NULL, -- Failure: TIT05                                            \n" +
        "  ts038tit06f      TINYINT                NOT NULL, -- Failure: TIT06                                            \n" +
        "  ts039tit07f      TINYINT                NOT NULL, -- Failure: TIT07                                            \n" +
        "  ts040tit08f      TINYINT                NOT NULL, -- Failure: TIT08                                            \n" +
        "  ts041tit09f      TINYINT                NOT NULL, -- Failure: TIT09                                            \n" +
        "  ts042tit10f      TINYINT                NOT NULL, -- Failure: TIT10                                            \n" +
        "  ts043tit11f      TINYINT                NOT NULL, -- Failure: TIT11                                            \n" +
      
        "  CONSTRAINT binary_ts CHECK (                                                                                   \n" +
        "        ts001prelay IN (0, 1)                                                                                    \n" +
        "    AND ts002p1hotf IN (0, 1)                                                                                    \n" +
        "    AND ts003p2hotf IN (0, 1)                                                                                    \n" +
        "    AND ts004p3hotf IN (0, 1)                                                                                    \n" +
        "    AND ts005p1heaf IN (0, 1)                                                                                    \n" +
        "    AND ts006p2heaf IN (0, 1)                                                                                    \n" +
        "    AND ts007p3heaf IN (0, 1)                                                                                    \n" +
        "    AND ts008p1recf IN (0, 1)                                                                                    \n" +
        "    AND ts009p2recf IN (0, 1)                                                                                    \n" +
        "    AND ts010p1cirf IN (0, 1)                                                                                    \n" +
        "    AND ts011p2cirf IN (0, 1)                                                                                    \n" +
        "    AND ts012drains IN (0, 1)                                                                                    \n" +
        "    AND ts013p1hots IN (0, 1)                                                                                    \n" +
        "    AND ts014p2hots IN (0, 1)                                                                                    \n" +
        "    AND ts015p3hots IN (0, 1)                                                                                    \n" +
        "    AND ts016p1heas IN (0, 1)                                                                                    \n" +
        "    AND ts017p2heas IN (0, 1)                                                                                    \n" +
        "    AND ts018p3heas IN (0, 1)                                                                                    \n" +
        "    AND ts019p1recs IN (0, 1)                                                                                    \n" +
        "    AND ts020p2recs IN (0, 1)                                                                                    \n" +
        "    AND ts021p1cirs IN (0, 1)                                                                                    \n" +
        "    AND ts022p2cirs IN (0, 1)                                                                                    \n" +
        "    AND ts023alarms IN (0, 1)                                                                                    \n" +
        "    AND ts024rchots IN (0, 1)                                                                                    \n" +
        "    AND ts025rcheas IN (0, 1)                                                                                    \n" +
        "    AND ts026rcrecs IN (0, 1)                                                                                    \n" +
        "    AND ts027rccirs IN (0, 1)                                                                                    \n" +
        "    AND ts028auhots IN (0, 1)                                                                                    \n" +
        "    AND ts029auheas IN (0, 1)                                                                                    \n" +
        "    AND ts030aurecs IN (0, 1)                                                                                    \n" +
        "    AND ts031aucirs IN (0, 1)                                                                                    \n" +
        "    AND ts032pwmons IN (0, 1)                                                                                    \n" +
        "    AND ts033tit01f IN (0, 1)                                                                                    \n" +
        "    AND ts034tit02f IN (0, 1)                                                                                    \n" +
        "    AND ts035tit03f IN (0, 1)                                                                                    \n" +
        "    AND ts036tit04f IN (0, 1)                                                                                    \n" +
        "    AND ts037tit05f IN (0, 1)                                                                                    \n" +
        "    AND ts038tit06f IN (0, 1)                                                                                    \n" +
        "    AND ts039tit07f IN (0, 1)                                                                                    \n" +
        "    AND ts040tit08f IN (0, 1)                                                                                    \n" +
        "    AND ts041tit09f IN (0, 1)                                                                                    \n" +
        "    AND ts042tit10f IN (0, 1)                                                                                    \n" +
        "    AND ts043tit11f IN (0, 1)                                                                                    \n" + 
        "  ),                                                                                                             \n" +
        
        "  PRIMARY KEY (timestamp, id)                                                                                    \n" +
        ") ENGINE = MergeTree() ORDER BY (timestamp, id);                                                                 \n" ;


    private final static Logger put = LoggerFactory.getLogger(MessageProcessor.class);
    private final static JSONParser jsonParser = new JSONParser();

    public void run() {
        /** First of all try to clear concurrent queue as faster as possible: */
        Object[] message_array = StmikServiceApp.message_queue.toArray();
        StmikServiceApp.message_queue.clear();

        put.info(MessageFormat.format("new {0} message(s) arrived", message_array.length));
        if (message_array.length < 1) {
            put.info(
                "Since no messages arrived no job for Message processor are present. " +
                "If Message processor is often stays without job (when no messages are arrived from the server), " +
                "try to increase the duration of the interval between jobs. As for now go waiting further..."
            );
            return;  
        }

        /** Connect to backend */
        try (Connection connection = DriverManager.getConnection(chConnectionString)) {
            put.info(MessageFormat.format("JDBC connection. Connection to <{0}> is confirmed", chConnectionString));
            try (Statement stmt = connection.createStatement()) {
                
                /** Recreate table */
                try ( ResultSet rs = stmt.executeQuery(MessageFormat.format(TABLE_CREATION_QUERY, backendTableName)) ) {
                    put.info("JDBC connection. CREATE TABLE query executed successfully. ");
                } catch (Exception e) {
                    put.error("JDBC connection. Cannot execute CREATE TABLE query. ");
                    e.printStackTrace();
                }

                /** Describe table*/
                int stmik_column_number = 2;  // default value 
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(name) FROM system.columns WHERE table='" + backendTableName + "';")) {
                    if (rs.next()) {
                        stmik_column_number = rs.getInt(1);
                    }
                }

                /** Insert data */
                try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO " + backendTableName + " VALUES(" + "?,".repeat(stmik_column_number - 1) + "?)")) {
                    JSONObject messageObj;
                    JSONArray tit, ts;
                    int field_cursor;
                    for (int j = 0; j < message_array.length; j++){
                        field_cursor = 0;
                        try {
                            /** Start message processing*/
                            messageObj = (JSONObject) jsonParser.parse( message_array[j].toString() );                                           
                         
                            field_cursor++;
                            pstmt.setTimestamp(field_cursor, new Timestamp((Long) messageObj.get("timestamp")));                                    
                            
                            field_cursor++;
                            pstmt.setInt(field_cursor, (int)(long) messageObj.get("kpd"));
                         
                            /** Process TITs */
                            tit = (JSONArray) messageObj.get("TIT");
                            for (int i = 0; i < tit.size(); i++) {
                                field_cursor++;
                                pstmt.setInt( field_cursor, (int) Math.round( ((Number) tit.get(i)).doubleValue() * 100 ) );
                            }

                            /** Process TS */
                            ts = (JSONArray) messageObj.get("TS");
                            char[] tsbit =  (int16bits((int)(long) ts.get(0), true) + int16bits((int)(long) ts.get(1), true) + 
                                             int16bits((int)(long) ts.get(2), true) + int16bits((int)(long) ts.get(3), true)).toCharArray();
                                           
                            for (int i = 0; i < 43; i++) {
                               field_cursor++;
                               pstmt.setByte( field_cursor, (byte) Character.getNumericValue(tsbit[i]));

                            }
                                                       
                            /** Finalize message processing*/
                            pstmt.addBatch();
                        } catch(Exception e) {
                         put.error("Web-socket. Cannot parse JSON-message");
                         e.printStackTrace();
                        }
                    }
                    pstmt.executeBatch();
                    put.info(MessageFormat.format("JDBC connection. Data insert is successful", chConnectionString));
                } catch (Exception e) {
                    put.error("JDBC connection. Cannot execute INSERT INTO query. ");
                    e.printStackTrace();
                }
                put.info(MessageFormat.format("JDBC connection. Finish sending querires to <{0}>", chConnectionString));
            }
        } catch (Exception e) {
            put.error(MessageFormat.format("JDBC connection. Connection to <{0}> failed!", chConnectionString));
            e.printStackTrace();
            System.exit(73);
        }   
    }

    /**
    * Returns a string that represents bits of two Big Endian allocated bytes of short integer value.
    *
    * @param  x      integer that must between -32 768 and 32 767 (16-bit short integer)
    * @param little  should they use Little Endian order
    * @return    string containing zeroes and ones.
    * @see       Integer.toBinaryString
    */
    public static String int16bits(int x, boolean little) throws IllegalArgumentException {
        if (x < -32_768 || x > 32_767) throw new IllegalArgumentException("Argument value is out of bounds [-32768; +32767] for 2-byte integer");
        String s = String.format("%32s", Integer.toBinaryString(x)).replace(' ', '0').substring(16, 32);
        return (little) ? new StringBuilder(s).reverse().toString() : s;

    }
}
    

