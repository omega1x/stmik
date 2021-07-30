package ru.sibgenco;

public class MessageProcessor implements Runnable {
    public void run() {
        StmikServiceApp.message_queue.clear(); 
        System.out.println("Message processor's job done - queue is cleared!"); 
    } 
}
