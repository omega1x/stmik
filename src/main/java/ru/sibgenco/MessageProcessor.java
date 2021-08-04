package ru.sibgenco;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.sql.*;

public class MessageProcessor implements Runnable {
    private final static Logger put = LoggerFactory.getLogger(MessageProcessor.class);
    private final static JSONParser jsonParser = new JSONParser();

    public void run() {
        
        // Try to clear concurrent queue as faster as possible
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
        
        
        /** Parse JSON*/
        Long kpd = 0L;
        try {
            JSONObject messageObj = (JSONObject) jsonParser.parse( message_array[0].toString() );
            kpd = (Long) messageObj.get("kpd");

        } catch(Exception e) {
            e.printStackTrace();
        }
        System.out.println(kpd);
        
        
        /** Write to target */
        // Authorization: 
        //> clickhouse-client --host=127.0.0.1 --port=9000 --database=default --user=default --password=poss

        try (Connection connection = DriverManager.getConnection("jdbc:clickhouse://127.0.0.1:9000/default?user=default&password=poss")) {
            try (Statement stmt = connection.createStatement()) {
                try (ResultSet rs = stmt.executeQuery("drop table if exists test_jdbc_example")) {
                    System.out.println(rs.next());
                } catch (Exception e) {e.printStackTrace();}
                try (ResultSet rs = stmt.executeQuery("create table test_jdbc_example(day Date, name String, age UInt8) Engine=Log")) {
                    System.out.println(rs.next());
                } catch (Exception e) {e.printStackTrace();}
                /*
                try (PreparedStatement pstmt = connection.prepareStatement("INSERT INTO test_jdbc_example VALUES(?, ?, ?)")) {
                    for (int i = 1; i <= 200; i++) {
                        pstmt.setDate(1, new Date(System.currentTimeMillis()));
                        if (i % 2 == 0)
                            pstmt.setString(2, "Zhang San" + i);
                        else
                            pstmt.setString(2, "Zhang San");
                        pstmt.setByte(3, (byte) ((i % 4) * 15));
                        System.out.println(pstmt);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                }
        
                try (PreparedStatement pstmt = connection.prepareStatement("select count(*) from test_jdbc_example where age>? and age<=?")) {
                    pstmt.setByte(1, (byte) 10);
                    pstmt.setByte(2, (byte) 30);
                    //System.out.println(pstmt);
                }
        
                try (PreparedStatement pstmt = connection.prepareStatement("select count(*) from test_jdbc_example where name=?")) {
                    pstmt.setString(1, "Zhang San");
                   // printCount(pstmt);
                }
                //try (ResultSet rs = stmt.executeQuery("drop table test_jdbc_example")) {
                //    System.out.println(rs.next());
                //}
            }
            */
        } catch (Exception e) {
            e.printStackTrace();
        }   
    }  catch (Exception e) { e.printStackTrace();}
}
    
}
