package ru.sibgenco;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STMIK - connector to BTSK telemetry service
 */
public final class StmikServiceApp {
    private StmikServiceApp() {
    }
    
    final static Logger put = LoggerFactory.getLogger(StmikServiceApp.class);
    /**
     * Executes ETL-operations
     * @param args The arguments of the program.
     */
    public static void main(String[] args) {
        put.info("Start *stmik* service");
        Client client = new Client();
        client.connect();

        //Пробуем последовательно запрашивать разные КП
        for (int i = 1; i <= 5; i++){
            client.sendMessage("{\"kpd\" : "+ i + "}");
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (Exception e){
            }
        }
        client.close();
        put.info("Finish *stmik* service");
    }
}
