package ru.sibgenco;

import java.text.MessageFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProcessor implements Runnable {
    private final static Logger put = LoggerFactory.getLogger(MessageProcessor.class);
    public void run() {
        // Try to clear queue as faster as possible
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

    } 
}
