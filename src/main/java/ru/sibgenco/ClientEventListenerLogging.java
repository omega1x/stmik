package ru.sibgenco;

import fr.bmartel.protocol.websocket.client.IWebsocketClientChannel;
import fr.bmartel.protocol.websocket.client.IWebsocketClientEventListener;
import java.io.UnsupportedEncodingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//логирующий слушатель клиентов
public class ClientEventListenerLogging implements IWebsocketClientEventListener {
    private final static Logger put = LoggerFactory.getLogger(ClientEventListenerLogging.class);
    @Override
    public void onSocketConnected() {
       put.info("Connection confirmed");
    }

    @Override
    public void onSocketClosed() {
        put.info("Socket close confirmed");
    }

    @Override
    public void onIncomingMessageReceived(byte[] data, IWebsocketClientChannel iWebsocketClientChannel) {
        try {
            StmikServiceApp.message_queue.add(new String(data, "UTF-8"));
        }
        catch (UnsupportedEncodingException e){
            put.error("Unsupported encoding for message is detected");
        }
    }

}
