package ru.sibgenco;

import fr.bmartel.protocol.websocket.client.IWebsocketClientChannel;
import fr.bmartel.protocol.websocket.client.IWebsocketClientEventListener;
// import org.apache.log4j.LogManager;
// import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;

//логирующий слушатель клиентов
public class ClientEventListenerLogging implements IWebsocketClientEventListener {

    // private static final Logger log4 = LogManager.getLogger("ClientListenerLogger");

    @Override
    public void onSocketConnected() {
        // log4.info("Клиент подключен");
        System.out.println("Клиент подключен");
    }

    @Override
    public void onSocketClosed() {
        // log4.info("Клиент отключен");
        System.out.println("Клиент отключен");
    }

    @Override
    public void onIncomingMessageReceived(byte[] data, IWebsocketClientChannel iWebsocketClientChannel) {
        try {
            // log4.info("Получено сообщение от сервера: " + new String(data, "UTF-8"));
            System.out.println("Получено сообщение от сервера: " + new String(data, "UTF-8"));
        }
        catch (UnsupportedEncodingException e){
            // log4.info("Получено сообщение от сервера: *проблемы с перекодировкой: " + e.getMessage());
            System.out.println("Получено сообщение от сервера: *проблемы с перекодировкой: " + e.getMessage());
        }
    }

}
