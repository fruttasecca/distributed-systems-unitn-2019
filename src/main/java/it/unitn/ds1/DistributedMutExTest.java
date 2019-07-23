package it.unitn.ds1;

import static org.junit.Assert.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;


public class DistributedMutExTest {

    /**
     * pause execution for a given period of time
     * @param secs
     */
    private void sleep(int secs) {
        try {
            TimeUnit.SECONDS.sleep(secs);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void createFile(String filename){
        //create history out file for debugging
        FileWriter fileWriter = null;
        try {
            fileWriter = new FileWriter(filename, false);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * test0: all nodes should receive initialization message
     */
    @org.junit.Test
    public void test0(){
        DistributedMutEx mutEx_run = new DistributedMutEx();
        mutEx_run.init();

        //initialize history file
        String outfile = "test0.txt";
        createFile(outfile);

        //        wait 2 seconds to complete init
        sleep(2);

        mutEx_run.printAllHist(outfile);
        boolean allReceivedInit = true;
        try {

            String history = new String(Files.readAllBytes(Paths.get(outfile)), StandardCharsets.UTF_8);
            for (int i = 0; i < mutEx_run.getnActors(); i++) {
                String received_init = String.format("Node %02d received Initialize msg from node", i);
                if (!(history.contains(received_init))) {
                    allReceivedInit = false;
                }
                System.out.println();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(allReceivedInit);

        mutEx_run.terminate();
    }


    /**
     * test1: a node requests access to the CS and a token should be eventually granted
     * within a limited period of time
     */
    @org.junit.Test
    public void test1() {

        DistributedMutEx mutEx_run = new DistributedMutEx();
        mutEx_run.init();

        //initialize history file
        String outfile = "test1.txt";
        createFile(outfile);

//        wait 2 seconds to complete init
        sleep(2);

        mutEx_run.request_cs(5);

        sleep(5);

        mutEx_run.printHist(5, outfile);
        boolean enteredCS = false;
        try {
            String history = new String(Files.readAllBytes(Paths.get(outfile)), StandardCharsets.UTF_8);

        } catch (IOException e) {
            e.printStackTrace();
        }

        assertEquals(true, enteredCS);

        mutEx_run.terminate();
    }

    /**
     * test2: a few nodes request access to the CS and a token should be eventually granted
     * to each of them within a limited period of time
     */
    @org.junit.Test
    public void test2() {

        DistributedMutEx mutEx_run = new DistributedMutEx();
        mutEx_run.init();

        //initialize history file
        String outfile = "test2.txt";
        createFile(outfile);

//        wait 2 seconds to complete init

        sleep(2);

        int[] nodes = {0, 3, 7, 8};
        for (int i : nodes) {
            mutEx_run.request_cs(i);
        }

        // wait for execution
        sleep(50);

        mutEx_run.printAllHist(outfile);

        // wait for all nodes to print their histories
        sleep(4);

        boolean enteredCS = true;

        try {

            String history = new String(Files.readAllBytes(Paths.get(outfile)), StandardCharsets.UTF_8);

            for (int i : nodes) {
                String entered = String.format("Node %02d entered CS", i);
                if (!(history.contains(entered))) {
                    enteredCS = false;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        assertTrue(enteredCS);
        mutEx_run.terminate();
    }

    /**
     * test3: node crash after init phase
     */
    @org.junit.Test
    public void test3() {
        DistributedMutEx mutEx_run = new DistributedMutEx();
        mutEx_run.init();

        //wait for init to complete
        sleep(2);

        mutEx_run.node_failure(3);

        sleep(10);
        mutEx_run.terminate();
    }

}