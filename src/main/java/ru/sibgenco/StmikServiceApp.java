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


@Command(name = "stmik",
         headerHeading = "%n@|bold Usage: |@%n%n",
         synopsisHeading = "%n",
         optionListHeading = "%n@|italic Options:|@%n",
         descriptionHeading = "%nDescription:%n%n",
         parameterListHeading = "%n@|italic Parameters:|@ %n",
         header = "Connect to @|bold,fg(blue) BTSK-telemetry service|@ through web-socket interface and store recieved telemetry data in @|bold,fg(blue) ClickHouse|@ backend database.%n",
         footer = "%n%n@|bold Details:|@%n%nData are received and recorded to backend database for unlimited period until the user will interrupt the process by @|underline,bold Ctrl+C|@.%n" +
                   "%nValues of the following parameters are recorded synchronously to table @|italic stmik_messenger|@ of the backend database:" + 
                   "%n%n@|italic,fg(245) Attributes:|@                                               " +                     
                   "%n  @|bold,fg(40) timestamp  |@  Epoch-Unix-timestamp of the record              " +
                   "%n  @|bold,fg(40) id         |@  District heating network object id              " +
                   
                   "%n%n@|italic,fg(245) Measurements:|@                                             " +
                   "%n  @|bold,fg(48) tit01heatt2|@  Temperature of return heating water, [°C]×100   " +
                   "%n  @|bold,fg(48) tit02netwt2|@  Temperature of return network water, [°C]×100   " +
                   "%n  @|bold,fg(48) tit03hotwt1|@  Temperature of hot water, [°C]×100              " +
                   "%n  @|bold,fg(48) tit04heatt1|@  Temperature of supply heating water, [°C]×100   " +
                   "%n  @|bold,fg(48) tit05netwt1|@  Temperature of supply network water, [°C]×100   " +
                   "%n  @|bold,fg(48) tit06utilp1|@  Pressure of water utility, [kg×f/cm²]×100       " +
                   "%n  @|bold,fg(48) tit07heatp2|@  Pressure of return heating water, [kg×f/cm²]×100" +
                   "%n  @|bold,fg(48) tit08hotwp1|@  Pressure of hot water, [kg×f/cm²]×100           " +
                   "%n  @|bold,fg(48) tit09heatp1|@  Pressure of supply heating water, [kg×f/cm²]×100" +
                   "%n  @|bold,fg(48) tit10netwp2|@  Pressure of return network water, [kg×f/cm²]×100" +
                   "%n  @|bold,fg(48) tit11netwp1|@  Pressure of supply network water, [kg×f/cm²]×100" +

                   "%n%n@|italic,fg(245) Flags: |@                                                   " +
                   "%n  @|bold,fg(51) ts001prelay|@  Switch of relay of the drainage pit (on/off)    " +
                   "%n  @|bold,fg(51) ts002p1hotf|@  Failure of hot water pump-1 (yes/no)            " +
                   "%n  @|bold,fg(51) ts003p2hotf|@  Failure of hot water pump-2 (yes/no)            " +
                   "%n  @|bold,fg(51) ts004p3hotf|@  Failure of hot water pump-3 (yes/no)            " +
                   "%n  @|bold,fg(51) ts005p1heaf|@  Failure of heating pump-1  (yes/no)             " +
                   "%n  @|bold,fg(51) ts006p2heaf|@  Failure of heating pump-2  (yes/no)             " +
                   "%n  @|bold,fg(51) ts007p3heaf|@  Failure of heating pump-3  (yes/no)             " +
                   "%n  @|bold,fg(51) ts008p1recf|@  Failure of recharge pump-1 (yes/no)             " +
                   "%n  @|bold,fg(51) ts009p2recf|@  Failure of recharge pump-2 (yes/no)             " +
                   "%n  @|bold,fg(51) ts010p1cirf|@  Failure of circulation pump-1 (yes/no)          " +
                   "%n  @|bold,fg(51) ts011p2cirf|@  Failure of circulation pump-2 (yes/no)          " +
                   "%n  @|bold,fg(51) ts012drains|@  Switch of drainage pump    (on/off)             " +
                   "%n  @|bold,fg(51) ts013p1hots|@  Switch of hot water pump-1 (on/off)             " +
                   "%n  @|bold,fg(51) ts014p2hots|@  Switch of hot water pump-2 (on/off)             " +
                   "%n  @|bold,fg(51) ts015p3hots|@  Switch of hot water pump-3 (on/off)             " +
                   "%n  @|bold,fg(51) ts016p1heas|@  Switch of heating pump-1   (on/off)             " +
                   "%n  @|bold,fg(51) ts017p2heas|@  Switch of heating pump-2   (on/off)             " +
                   "%n  @|bold,fg(51) ts018p3heas|@  Switch of heating pump-3   (on/off)             " +
                   "%n  @|bold,fg(51) ts019p1recs|@  Switch of recharge pump-1  (on/off)             " +
                   "%n  @|bold,fg(51) ts020p2recs|@  Switch of recharge pump-2  (on/off)             " +
                   "%n  @|bold,fg(51) ts021p1cirs|@  Switch of circulation pump-1    (on/off)        " +
                   "%n  @|bold,fg(51) ts022p2cirs|@  Switch of circulation pump-2    (on/off)        " +
                   "%n  @|bold,fg(51) ts023alarms|@  Switch of security alarm system (on/off)        " +
                   "%n  @|bold,fg(51) ts024rchots|@  Switch of remote control of hot water   (on/off)" +
                   "%n  @|bold,fg(51) ts025rcheas|@  Switch of remote control of heating     (on/off)" +
                   "%n  @|bold,fg(51) ts026rcrecs|@  Switch of remote control of recharge    (on/off)" +
                   "%n  @|bold,fg(51) ts027rccirs|@  Switch of remote control of circulation (on/off)" +
                   "%n  @|bold,fg(51) ts028auhots|@  Switch of hot water automation    (on/off)      " +
                   "%n  @|bold,fg(51) ts029auheas|@  Switch of heating automation      (on/off)      " +
                   "%n  @|bold,fg(51) ts030aurecs|@  Switch of recharge automation     (on/off)      " +
                   "%n  @|bold,fg(51) ts031aucirs|@  Switch of circulation automation  (on/off)      " +
                   "%n  @|bold,fg(51) ts032pwmons|@  Switch of power supply monitoring (on/off)      " +
                   "%n  @|bold,fg(51) ts033tit01f|@  Failure of TIT01 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts034tit02f|@  Failure of TIT02 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts035tit03f|@  Failure of TIT03 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts036tit04f|@  Failure of TIT04 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts037tit05f|@  Failure of TIT05 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts038tit06f|@  Failure of TIT06 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts039tit07f|@  Failure of TIT07 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts040tit08f|@  Failure of TIT08 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts041tit09f|@  Failure of TIT09 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts042tit10f|@  Failure of TIT10 sensor (yes/no)                " +
                   "%n  @|bold,fg(51) ts043tit11f|@  Failure of TIT11 sensor (yes/no)                " +
                   "%n                                                                               "
                   ,
         exitCodeListHeading = "%n@|italic Exit Codes: |@%n",
         exitCodeList = { // look further at EXIT_CODE field of this class
             " @|bold  0 |@:Successful program execution.",
             " @|bold  1 |@:Usage error: user input for the command was incorrect.",
             " @|bold 70 |@:Program is terminated by user.",
             " @|bold 71 |@:Internal software error: an unexpected exception occurred.",
             " @|bold 72 |@:Internal software error: web-socket client cannot be created.",
             " @|bold 73 |@:Connection error: service unavailable."
            },
         sortOptions = false,
         mixinStandardHelpOptions = true, 
            version = "stmik 0.9.0"
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
    private final static String CLICKHOUSE_TABLE_NAME = "stmik_messenger";  //Possibly, it should be a command line option
    
    /** Command line options */
    @Spec CommandSpec spec; // injected by picocli
    
    // CLIENT_KEY_FILE_NAME_PATH
    @Option(names = {"-k", "--key-file"}, paramLabel = "FILE", required = true, order = 1, 
            description = "Public key @|italic PKCS12|@-file provided for protected connection with @|bold,fg(blue) BTSK-telemetry service|@.")
    private static String CLIENT_KEY_FILE_NAME_PATH;
    
    // CLIENT_KEY_PASSWORD
    @Option(names = {"-p", "--password"}, paramLabel = "PASSWORD", required = true, order = 2,
            description = "Public key password provided with @|italic PKCS12|@-file.")
    private static String CLIENT_KEY_PASSWORD;
    
    // OBJECT_RESPOND_INTERVAL
    private final static int MAX_ACQUIRE_SESSION_INTERVAL = 1700;  // [seconds]
    private final static int MIN_ACQUIRE_SESSION_INTERVAL =   60;  // [seconds]
    private final static int DEF_ACQUIRE_SESSION_INTERVAL =  300;  // [seconds]
    @Option(names = {"-a", "--acquire"}, paramLabel = "SECONDS", defaultValue = "" + DEF_ACQUIRE_SESSION_INTERVAL, order = 3,
            description = "interval between acquire sessions initiated by @|bold,fg(blue) BTSK-telemetry service|@ in seconds. Must be greater than " + 
            MIN_ACQUIRE_SESSION_INTERVAL + " and less then " + MAX_ACQUIRE_SESSION_INTERVAL + 
            ". Default value is " + DEF_ACQUIRE_SESSION_INTERVAL + " seconds.")
    private static int ACQUIRE_SESSION_INTERVAL;
    
    // MESSAGE_PROCESSOR_PERIOD
    private final static int DEF_TRANSFER_SESSION_INTERVAL = (int) (DEF_ACQUIRE_SESSION_INTERVAL * 1.8);
    @Option(names = {"-t", "--transfer"}, paramLabel = "SECONDS", defaultValue = "" + DEF_TRANSFER_SESSION_INTERVAL, order = 4,
            description = "interval between transfer sessions to backend database in seconds. Should be greater than interval between acquire sessions. " +
                          "Default value is " + DEF_TRANSFER_SESSION_INTERVAL + " seconds"
            )
    private static long TRANSFER_SESSION_INTERVAL;     //[seconds]
    
    // CLICKHOUSE_CONNECTION_STRING
    @Parameters(paramLabel = "BACKEND", 
                description = "@|italic jdbc|@-connection string to backend @|bold,fg(blue) Clickhouse|@ database. Default connection is made to local host with the next @|italic jdbc|@-connection string: @|italic jdbc:clickhouse://127.0.0.1:9000/default?user=default&password=default|@.",
                defaultValue = "jdbc:clickhouse://127.0.0.1:9000/default?user=default&password=default"
                )
    private static String CLICKHOUSE_CONNECTION_STRING; // = "jdbc:clickhouse://127.0.0.1:9000/default?user=default&password=default";  // |> clickhouse-client --host=127.0.0.1 --port=9000 --user=default --password=default
    
    
    /* Exploited objects */
    private final static Logger put = LoggerFactory.getLogger(StmikServiceApp.class);
    private final static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
    private final static WebsocketClient WsClient = new WebsocketClient(STMIK_SERVER_ADDRESS, STMIK_SERVER_PORT);
    public final static ConcurrentLinkedQueue<String> message_queue = new ConcurrentLinkedQueue<String>();
    private static String ACQUISITION_QUERY; 

    @Override
    public Integer call() throws Exception {
        /**  Custom validation of command line parameters */
        if (ACQUIRE_SESSION_INTERVAL <= MIN_ACQUIRE_SESSION_INTERVAL || ACQUIRE_SESSION_INTERVAL >= MAX_ACQUIRE_SESSION_INTERVAL) {
            throw new ParameterException(
                spec.commandLine(), 
                MessageFormat.format("Error in option value: {0} is an invalid value of --interval option", ACQUIRE_SESSION_INTERVAL)
            );
        } else {
            ACQUISITION_QUERY = "{" + 
                MessageFormat.format("\"kpd\" : \"All\", \"interval\" : {0}", ACQUIRE_SESSION_INTERVAL) + "}";
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
            new MessageProcessor(CLICKHOUSE_CONNECTION_STRING, CLICKHOUSE_TABLE_NAME), 
            MESSAGE_PROCESSOR_START_DELAY, TRANSFER_SESSION_INTERVAL, TimeUnit.SECONDS
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
