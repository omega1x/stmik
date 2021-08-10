package ru.sibgenco;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for simple App.
 */
class AppTest {
    /**
     * Rigorous Test.
     */
   
    @Test
    void ValidIntTo16BitBigEndianConversion() {
        assertEquals(
            MessageProcessor.int16bits(8192, false) + MessageProcessor.int16bits(-14240, false) + 
            MessageProcessor.int16bits( 328, false) + MessageProcessor.int16bits(    79, false),
            "0010000000000000110010000110000000000001010010000000000001001111"
        );
    }

    @Test
    void ValidIntTo16BitLittleEndianConversion() {
        assertEquals(
            MessageProcessor.int16bits(10240, true) + MessageProcessor.int16bits(-32736, true) + 
            MessageProcessor.int16bits( 4096, true) + MessageProcessor.int16bits(     7, true),  
            "0000000000010100000001000000000100000000000010001110000000000000"
        );
    }
}
