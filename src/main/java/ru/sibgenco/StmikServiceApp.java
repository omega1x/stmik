package ru.sibgenco;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.io.File;
//import java.net.URL;
import java.nio.file.Paths;
import java.text.MessageFormat;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.bmartel.protocol.websocket.client.WebsocketClient;

/**
 * STMIK - connector to BTSK telemetry service
 */
public final class StmikServiceApp {
    private StmikServiceApp() {
    }
    
    // TODO: get the next stuff as command line arguments:
    private final static String CLIENT_KEY_FILE_NAME_PATH = "/home/poss/Documents/FreeSoul/stmik/src/main/resources/acservice01.p12";
    private final static String CLIENT_KEY_PASSWORD = "iOEWS3DTue";
    
    /* Static application configuration */
    private static String STMIK_SERVER_ADDRESS = "ctp.stmik.ru";
    private static int STMIK_SERVER_PORT = 1515;

    private final static String SERVER_KEY_FILE_NAME = "stmik-server.jks";
    private final static String SERVER_KEY_PASSWORD = "123456";
    private final static String SERVER_KEY_FULL_PATH = Paths.get(".").toAbsolutePath().normalize().toString() + "/" + SERVER_KEY_FILE_NAME;

    private final static String CLIENT_CERTIFICATE_TYPE = "PKCS12";
    private final static String SERVER_CERTIFICATE_TYPE = "JKS";
    private final static String SSL_PROTOCOL_TYPE = "TLS";

    private final static Map<String, Integer> EXIT_CODE;
    static {
        Map<String, Integer> list = new HashMap<String, Integer>();
        list.put("All done, thanks!", 0);
        list.put("Client fail", 2);
        list.put("Connection fail", 3);
        EXIT_CODE = Collections.unmodifiableMap(list);
    }

    /* Class subobjects */
    private final static Logger put = LoggerFactory.getLogger(StmikServiceApp.class);
    private static WebsocketClient Client;

    /**
     * Executes ETL-operations
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        put.info("Start *stmik* service");

        /* Prepare server certificate: */
        put.info(MessageFormat.format("Extract server certificate <{0}>", SERVER_KEY_FILE_NAME));
        // URL serverKeyResourcePath = StmikServiceApp.class.getResource("/" + SERVER_KEY_FILE_NAME);
        try {
          FileUtils.copyURLToFile(StmikServiceApp.class.getResource("/" + SERVER_KEY_FILE_NAME), new File(SERVER_KEY_FULL_PATH));
        } catch (Exception x) { x.printStackTrace(); }
        put.info(MessageFormat.format("Ok. Server certificate successfully extracted to <{0}>", SERVER_KEY_FULL_PATH));
        
        /* Initiate web-socket connection: */
        put.info(MessageFormat.format("Create web-socket client to <{0}:{1}>", STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT));
        /* Client client = new Client(
            STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT, SERVER_KEY_FULL_PATH, SERVER_KEY_PASSWORD, 
            CLIENT_KEY_FILE_NAME_PATH, CLIENT_KEY_PASSWORD
        ); */
        Client = new WebsocketClient(STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT);
        Client.setSsl(true);
        Client.setSSLParams(
            CLIENT_CERTIFICATE_TYPE, SERVER_CERTIFICATE_TYPE, CLIENT_KEY_FILE_NAME_PATH,
            SERVER_KEY_FULL_PATH, SSL_PROTOCOL_TYPE, CLIENT_KEY_PASSWORD, SERVER_KEY_PASSWORD
        );

        if (Client == null){
            put.error(
                MessageFormat.format(
                    "Fail to create web-socket client. Program is terminated with exit code {0}", 
                    EXIT_CODE.get("Client fail")));
            System.exit(EXIT_CODE.get("Client fail"));
        }    
        put.info("Ok. Client is successfully created.");
        put.info(MessageFormat.format("Connect to <{0}:{1}>", STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT));
        Client.addClientSocketEventListener(new ClientEventListenerLogging());
        Client.connect();
        if (!Client.isConnected()) {
            put.error(MessageFormat.format("Fail to connect to <{0}:{1}>. Program is terminated with exit code {2}",
            STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT, EXIT_CODE.get("Connection fail")));
            System.exit(EXIT_CODE.get("Connection fail"));
        }
        put.info(
            MessageFormat.format(
                "Ok. Successfully connected to <{0}:{1}>",
                STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT
            )
        );   

        put.info("Start sending and recieve messages");
        for (int i = 1; i <= 5; i++){
            Client.writeMessage("{\"kpd\" : "+ i + "}");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception e){ }
        }
        put.info("Finish sending and recieve messages");
        Client.closeSocket();
        Client.cleanEventListeners();
        put.info(
            MessageFormat.format(
                "Connection to <{0}:{1}> is closed",
                STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT
            )
        );   
        put.info(
            MessageFormat.format(
                "Ok. *stmik* finishes the service. The program returns exit code {0}",
                EXIT_CODE.get("All done, thanks!")
            )
        );
        System.exit(EXIT_CODE.get("All done, thanks!"));   
    }
}
