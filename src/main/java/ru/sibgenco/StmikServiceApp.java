package ru.sibgenco;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.*;
// import picocli.CommandLine.Parameters;

import java.util.Map;
import java.util.HashMap;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.File;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.concurrent.Callable;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.bmartel.protocol.websocket.client.WebsocketClient;

@Command(name = "stmik", mixinStandardHelpOptions = true, version = "stmik 1.0",
         header = "@|bold Connect to|@ @|bold,fg(blue) BTSK|@ @|bold telemetry service through web-socket interface. |@%n", 
         footer = "%nData are received and transferred for unlimited period until the user will interrupt the process by @|underline,bold Ctrl+C|@.%n")
class StmikServiceApp implements Callable<Integer> {
    private StmikServiceApp() {
    }
    @Spec CommandSpec spec; // injected by picocli

    @Option(names = {"-k", "--client-key"}, paramLabel = "FILE", required = true, 
            description = "PKCS12 file provided by @|fg(blue),bold BTSK|@.")
    private static String CLIENT_KEY_FILE_NAME_PATH;
    
    @Option(names = {"-p", "--password"}, paramLabel = "PASSWORD", required = true, 
            description = "password provided with PKCS12 file.")
    private static String CLIENT_KEY_PASSWORD;

    private final static int MAX_OBJECT_RESPOND_INTERVAL = 1700;  // [seconds]
    private final static int MIN_OBJECT_RESPOND_INTERVAL =   60;  // [seconds]
    private final static int DEF_OBJECT_RESPOND_INTERVAL =  300;  // [seconds]
    
    @Option(names = {"-i", "--interval"}, paramLabel = "SECONDS", defaultValue = "" + DEF_OBJECT_RESPOND_INTERVAL, 
            description = "Object respond interval in seconds. Must be greater than " + 
            MIN_OBJECT_RESPOND_INTERVAL + " and less then " + MAX_OBJECT_RESPOND_INTERVAL + 
            ". Default value is " + DEF_OBJECT_RESPOND_INTERVAL + " seconds.")
    private static int OBJECT_RESPOND_INTERVAL;
        
    
    /* Hard-coded application configuration */
    private final static String STMIK_SERVER_ADDRESS = "ctp.stmik.ru";
    private final static int STMIK_SERVER_PORT = 1515;

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
    private final static String ACQUISITION_QUERY = "{" + MessageFormat.format(
        "\"kpd\" : \"All\", \"interval\" : {0}", OBJECT_RESPOND_INTERVAL
    ) + "}";
    private final static int CONNECTION_LIFE_CYCLE = 175_200;  // [hours], i.e. infinity

    /* Class subobjects */
    private final static Logger put = LoggerFactory.getLogger(StmikServiceApp.class);
    private static WebsocketClient Client;

    public static ConcurrentLinkedQueue<Integer> message_queue = new ConcurrentLinkedQueue<Integer>();

    @Override
    public Integer call() throws Exception {
        /* Custom validation of command line parameters */
        if (OBJECT_RESPOND_INTERVAL <= MIN_OBJECT_RESPOND_INTERVAL || OBJECT_RESPOND_INTERVAL >= MAX_OBJECT_RESPOND_INTERVAL) {
            throw new ParameterException(
                spec.commandLine(), 
                MessageFormat.format("Error in option value: {0} is an invalid value of --interval option", OBJECT_RESPOND_INTERVAL)
            );
        }    

        put.info("Start *stmik* service");
        /* Prepare server certificate: */
        put.info(MessageFormat.format("Extract server certificate <{0}>", SERVER_KEY_FILE_NAME));
        // URL serverKeyResourcePath = StmikServiceApp.class.getResource("/" + SERVER_KEY_FILE_NAME);
        try {
          FileUtils.copyURLToFile(StmikServiceApp.class.getResource("/" + SERVER_KEY_FILE_NAME), new File(SERVER_KEY_FULL_PATH));
        } catch (Exception x) { x.printStackTrace(); }
        put.info(MessageFormat.format("Ok. Server certificate successfully extracted to <{0}>", SERVER_KEY_FULL_PATH));
        

        /* Initiate web-socket connection: */
        put.info(MessageFormat.format("Create web-socket client to <{0}:{1}>", STMIK_SERVER_ADDRESS, Long.toString(STMIK_SERVER_PORT)));
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
            return(EXIT_CODE.get("Client fail"));
        }    
        put.info("Ok. Client is successfully created.");
        put.info(MessageFormat.format("Connect to <{0}:{1}>", STMIK_SERVER_ADDRESS, Long.toString(STMIK_SERVER_PORT)));
        Client.addClientSocketEventListener(new ClientEventListenerLogging());
        Client.connect();
        if (!Client.isConnected()) {
            put.error(MessageFormat.format("Fail to connect to <{0}:{1}>. Program is terminated with exit code {2}",
            STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT, EXIT_CODE.get("Connection fail")));
            return(EXIT_CODE.get("Connection fail"));
        }
        put.info(
            MessageFormat.format(
                "Ok. Successfully connected to <{0}:{1}>",
                STMIK_SERVER_ADDRESS, Long.toString(STMIK_SERVER_PORT)
            )
        );   
        
        put.info(
            MessageFormat.format(
                "Start sending and recieve messages sending acquisition query <{0}>", 
                ACQUISITION_QUERY
            )
        );
        Client.writeMessage(ACQUISITION_QUERY);
        try {
            TimeUnit.HOURS.sleep(CONNECTION_LIFE_CYCLE);
        } catch (Exception e){ }

        put.info("Finish sending and recieve messages");
        Client.cleanEventListeners();
        
        put.info(
            MessageFormat.format(
                "Connection to <{0}:{1}> will be closed at programm termination",
                STMIK_SERVER_ADDRESS, Long.toString(STMIK_SERVER_PORT)
            )
        );   
        put.info(
            MessageFormat.format(
                "Ok. *stmik* finishes the service. The program returns exit code {0}",
                EXIT_CODE.get("All done, thanks!")
            )
        );
        return(EXIT_CODE.get("All done, thanks!"));   
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new StmikServiceApp()).execute(args);
        System.exit(exitCode);
    }
}
