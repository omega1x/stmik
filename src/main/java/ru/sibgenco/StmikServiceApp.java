package ru.sibgenco;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * STMIK - ETL-service for transfering real-time data from BTSK district objects 
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
        put.info("Finish *stmik* service");
    }
}
