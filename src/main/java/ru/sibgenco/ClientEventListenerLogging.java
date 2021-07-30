package ru.sibgenco;

import fr.bmartel.protocol.websocket.client.IWebsocketClientChannel;
import fr.bmartel.protocol.websocket.client.IWebsocketClientEventListener;
import java.io.UnsupportedEncodingException;

//логирующий слушатель клиентов
public class ClientEventListenerLogging implements IWebsocketClientEventListener {

    // private static final Logger log4 = LogManager.getLogger("ClientListenerLogger");

    @Override
    public void onSocketConnected() {
        // log4.info("Клиент подключен");
        //System.out.println("Клиент подключен");
    }

    @Override
    public void onSocketClosed() {
        // log4.info("Клиент отключен");
        //System.out.println("Клиент отключен");
    }

    @Override
    public void onIncomingMessageReceived(byte[] data, IWebsocketClientChannel iWebsocketClientChannel) {
        try {
            String a = "Pong: " + new String(data, "UTF-8");
            a.isBlank();
            StmikServiceApp.message_queue.add(1);
            System.out.println("Messages in queue: " + StmikServiceApp.message_queue.size());
        }
        catch (UnsupportedEncodingException e){
            System.out.println("Pong problem: " + e.getMessage());
        }
    }

}
