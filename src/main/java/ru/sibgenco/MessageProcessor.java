package ru.sibgenco;

import java.lang.Math;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONArray;

import java.sql.*;

public class MessageProcessor implements Runnable {
    public MessageProcessor(String chConnectionString){
        this.chConnectionString = chConnectionString;
    }
    private String chConnectionString; //= ; 
    // TODO: Add field descriptions in SQL-comments
    private final static String TABLE_CREATION_QUERY =       
        "CREATE TABLE IF NOT EXISTS stmik (                                                                               \n" +
        "  timestamp  DateTime('Asia/Krasnoyarsk') NOT NULL, -- Epoch Unix timestamp (See https://www.unixtimestamp.com)  \n" +
        "  id         INTEGER                      NOT NULL, -- District heating network id                               \n" +
        "  TIT01      INTEGER                      NOT NULL, -- Temperature of the return heating water, [°C×100].        \n" +
        "  TIT02      INTEGER                      NOT NULL,          \n" +
        "  TIT03      INTEGER                      NOT NULL,          \n" +
        "  TIT04      INTEGER                      NOT NULL,          \n" +
        "  TIT05      INTEGER                      NOT NULL,          \n" +
        "  TIT06      INTEGER                      NOT NULL,          \n" +
        "  TIT07      INTEGER                      NOT NULL,          \n" +
        "  TIT08      INTEGER                      NOT NULL,          \n" +
        "  TIT09      INTEGER                      NOT NULL,          \n" +
        "  TIT10      INTEGER                      NOT NULL,          \n" +
        "  TIT11      INTEGER                      NOT NULL,          \n" +
        "  PRIMARY KEY (timestamp, id)                                                                                    \n"+
        ") ENGINE = MergeTree() ORDER BY (timestamp, id);                                                                       \n" ;


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
                try (ResultSet rs = stmt.executeQuery(TABLE_CREATION_QUERY)) {} catch (Exception e) {
                    put.error("JDBC connection. Cannot execute CREATE TABLE query. ");
                    e.printStackTrace();
                }

                /** Insert data */
                try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO stmik VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)")) {
                    JSONObject messageObj;
                    JSONArray tit;
                    for (int j = 0; j < message_array.length; j++){
                        try {
                            /** Start message processing*/
                            messageObj = (JSONObject) jsonParser.parse( message_array[j].toString() );                                           
                         
                            pstmt.setTimestamp(1, new Timestamp((Long) messageObj.get("timestamp")));                                    
                            pstmt.setInt(2, (int)(long) messageObj.get("kpd"));
                         
                            /** Process TITs */
                            tit = (JSONArray) messageObj.get("TIT");
                            for (int i = 0; i < tit.size(); i++) {
                               pstmt.setInt( 3 + i, (int) Math.round( ((Number) tit.get(i)).doubleValue() * 100 ) );
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
        }   
    }
}
    

