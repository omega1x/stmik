package ru.sibgenco;

import java.io.File;
import java.nio.file.Paths;
import java.text.MessageFormat;

import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.*;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import fr.bmartel.protocol.websocket.client.WebsocketClient;


@Command(name = "stmik", mixinStandardHelpOptions = true, version = "stmik 0.1",
         header = "@|bold Connect to|@ @|bold,fg(blue) BTSK|@ @|bold telemetry service through web-socket interface. |@%n", 
         footer = "%nData are received and transferred for unlimited period until the user will interrupt the process by @|underline,bold Ctrl+C|@.%n",
         exitCodeListHeading = "%nExit Codes:%n",
         exitCodeList = { // look further at EXIT_CODE field of this class
             " 0:Successful program execution.",
             " 1:Usage error: user input for the command was incorrect.",
             "70:Program is terminated by user.",
             "71:Internal software error: an unexpected exception occurred.",
             "72:Internal software error: web-socket client cannot be created.",
             "73:Connection error: service unavailable."
            }
         )
class StmikServiceApp implements Callable<Integer> {
    
    /* Embedded  configuration */
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
        list.put("User termination", 70);
        list.put("Unexpected exception", 71);
        list.put("Client fail", 72);
        list.put("Connection fail", 73);
        EXIT_CODE = Collections.unmodifiableMap(list);
    }
    private final static long CONNECTION_LIFE_CYCLE = 175_200;  // [hours], i.e. infinity
    private final static long MESSAGE_PROCESSOR_START_DELAY = 5L; //[seconds]
    
    // TODO: make it a command line option
    private final static long MESSAGE_PROCESSOR_PERIOD = 20L;     //[seconds]
    
    /* Command line options */
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
    private static String ACQUISITION_QUERY;  
    
    /* Exploited objects */
    private final static Logger put = LoggerFactory.getLogger(StmikServiceApp.class);
    private final static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final static WebsocketClient WsClient = new WebsocketClient(STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT);

    public final static ConcurrentLinkedQueue<String> message_queue = new ConcurrentLinkedQueue<String>();
    

    @Override
    public Integer call() throws Exception {
        /**  Custom validation of command line parameters */
        if (OBJECT_RESPOND_INTERVAL <= MIN_OBJECT_RESPOND_INTERVAL || OBJECT_RESPOND_INTERVAL >= MAX_OBJECT_RESPOND_INTERVAL) {
            throw new ParameterException(
                spec.commandLine(), 
                MessageFormat.format("Error in option value: {0} is an invalid value of --interval option", OBJECT_RESPOND_INTERVAL)
            );
        } else {
            ACQUISITION_QUERY = "{" + 
                MessageFormat.format("\"kpd\" : \"All\", \"interval\" : {0}", OBJECT_RESPOND_INTERVAL) + "}";
        }

        /**  Process interruption action from user */
        sun.misc.Signal.handle(new sun.misc.Signal("INT"),  signal -> {
            put.info("User interruption signal by Ctrl+C is recieved");
            put.info("Send stop signal to message processor");
            scheduledExecutorService.shutdown();
            if (scheduledExecutorService.isShutdown()) {
                put.info("Message processor has been shutdown");
            } else {
                put.warn("Message processor has not been shutdown");
            }
            put.info("Remove web-socket listeners");
            WsClient.cleanEventListeners();
            WsClient.closeSocket();
            put.info("Ready to terminate. Bye-bye!");
            System.exit(EXIT_CODE.get("User termination"));
        });

        /** Execute main thread */
        put.info("Start *stmik* service");
        
        /**  Prepare server certificate: */
        put.info(MessageFormat.format("Extract server certificate <{0}>", SERVER_KEY_FILE_NAME));
        try {
          FileUtils.copyURLToFile(StmikServiceApp.class.getResource("/" + SERVER_KEY_FILE_NAME), new File(SERVER_KEY_FULL_PATH));
        } catch (Exception x) { x.printStackTrace(); }
        put.info(MessageFormat.format("Ok. Server certificate successfully extracted to <{0}>", SERVER_KEY_FULL_PATH));
        

        /** Initiate web-socket connection: */
        put.info(MessageFormat.format("Create web-socket client to <{0}:{1}>", STMIK_SERVER_ADDRESS, Long.toString(STMIK_SERVER_PORT)));
       
        WsClient.setSsl(true);
        WsClient.setSSLParams(
            CLIENT_CERTIFICATE_TYPE, SERVER_CERTIFICATE_TYPE, CLIENT_KEY_FILE_NAME_PATH,
            SERVER_KEY_FULL_PATH, SSL_PROTOCOL_TYPE, CLIENT_KEY_PASSWORD, SERVER_KEY_PASSWORD
        );

        if (WsClient == null){
            put.error(
                MessageFormat.format(
                    "Fail to create web-socket client. Program is terminated with exit code {0}", 
                    EXIT_CODE.get("Client fail")));
            return(EXIT_CODE.get("Client fail"));
        }    
        put.info("Ok. Client is successfully created");

        put.info("Initiate message processor");
        ScheduledFuture<?> messageProccessorSchedule = scheduledExecutorService.scheduleWithFixedDelay(
            new MessageProcessor(), MESSAGE_PROCESSOR_START_DELAY, MESSAGE_PROCESSOR_PERIOD, TimeUnit.SECONDS
        );
        put.info("Message processors status is " + !messageProccessorSchedule.isCancelled());

        put.info(MessageFormat.format("Connect to <{0}:{1}>", STMIK_SERVER_ADDRESS, Long.toString(STMIK_SERVER_PORT)));
        WsClient.addClientSocketEventListener(new ClientEventListenerLogging());
        WsClient.connect();
        if (!WsClient.isConnected()) {
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
        WsClient.writeMessage(ACQUISITION_QUERY);
        try {
            TimeUnit.HOURS.sleep(CONNECTION_LIFE_CYCLE);
        } catch (Exception e){
            put.error("Main thread cannot be put on sleep.");
            return EXIT_CODE.get("Unexpected exception");
        }

        /** Finilize the thread (it is a practically unreachable code */
        put.info("Finish sending and recieve messages");
        WsClient.cleanEventListeners();
        
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
