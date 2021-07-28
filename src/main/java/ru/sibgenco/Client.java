package ru.sibgenco;

import fr.bmartel.protocol.websocket.client.WebsocketClient;
import fr.bmartel.protocol.websocket.server.IWebsocketClient;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class Client {
    private static String HOSTNAME = "ctp.stmik.ru";
    private  static int PORT = 1515;

    private final static String KEYSTORE_DEFAULT_TYPE = "PKCS12";
    private final static String TRUSTORE_DEFAULT_TYPE = "JKS";
    private final static String KEYSTORE_FILE_PATH = "/home/poss/Documents/FreeSoul/stmik/src/main/resources/acservice01.p12";
    private final static String TRUSTORE_FILE_PATH = "/home/poss/Documents/FreeSoul/stmik/src/main/resources/ctp_TRUSTORE.jks";
    private final static String SSL_PROTOCOL = "TLS";
    private final static String KEYSTORE_PASSWORD = "iOEWS3DTue";
    private final static String TRUSTORE_PASSWORD = "123456";

    //private WebsocketServer serverSocket = null;
    private WebsocketClient clientSocket;

    private static final Logger log4 = LogManager.getLogger("ClientLogger");

    public Client(){
        clientSocket = new WebsocketClient(HOSTNAME, PORT);
        clientSocket.setSsl(true);
        clientSocket.setSSLParams(KEYSTORE_DEFAULT_TYPE, TRUSTORE_DEFAULT_TYPE, KEYSTORE_FILE_PATH, TRUSTORE_FILE_PATH, SSL_PROTOCOL, KEYSTORE_PASSWORD, TRUSTORE_PASSWORD);

        if (clientSocket != null)
            System.out.println("Клиент успешно создан");
        else
            System.out.println("Клиент не был создан!");
    }

    public void connect (){
        clientSocket.addClientSocketEventListener(new ClientEventListenerLogging());   //добавляем логирующего слушателя
        clientSocket.connect();
        System.out.println("Соединение установлено");
    }

    public void sendMessage (String message){
        if (clientSocket.isConnected()){
            clientSocket.writeMessage(message);
            // log4.info("Отправлено сообщение: " + message);
            System.out.println("Отправлено сообщение: " + message);
        }
        else{
            // log4.error("Нет соединения с сервером!");
            System.out.println("Нет соединения с сервером!");
        }

    }

    public void close (){
        clientSocket.closeSocket();
        clientSocket.cleanEventListeners();
        log4.info("Соединение закрыто");
    }

    @Override
    protected void finalize (){
        log4.info("Клиент завершает работу");
    }

    //подключился клиент
    public void onClientConnection (IWebsocketClient client){
        log4.info("onClientConnection");
    }

    public void onClientClose(IWebsocketClient client) {
        //server.onClientClose(client);
        log4.info("onClientClose");
    }

}

