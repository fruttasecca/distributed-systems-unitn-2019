package it.unitn.ds1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class DistributedMutExMain {

    public static void main(String[] args) {
        DistributedMutEx mutEx_run = new DistributedMutEx();
        mutEx_run.init();

        boolean exit = false;
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(System.in));

        String user_in = new String();
        String action = new String();
        Integer action_node = new Integer(1);
        boolean valid_in = true;

        while (!exit) {
            valid_in = true;
            try {
                System.out.println(">>> Type <action><nodeId> in order to say what you want to do <<<");
                System.out.println(">>> Actions: 'c', ask for CS ; 'f', fail <<<");
                System.out.printf(">>> Node ids: 0 - %d <<<\n", mutEx_run.getnActors()-1);
                System.out.println(">>> Or type 'e' to exit <<<\n");

                user_in = reader.readLine();

            } catch (IOException ioe) {
            }

            StringTokenizer st = new StringTokenizer(user_in);
            while (st.hasMoreElements()) {

                String next_cmd = st.nextElement().toString();

                if (next_cmd.length()>0) {
                    action = next_cmd.substring(0, 1);

                    if (!action.equals("e"))
                        action_node = Integer.parseInt(next_cmd.substring(1, next_cmd.length()));
                }
                else
                    valid_in = false;

                if ((!(action.equals("c")) && !(action.equals("f")) && !(action.equals("e"))) || (action_node < 0 || action_node >= mutEx_run.getnActors() ))
                    valid_in = false;

                if (!valid_in)
                    System.out.println(">>> Not valid action code <<<");
                else {
                    switch (action){
                        case "e":
                            exit = true;
                            break;
                        case "c":
                            mutEx_run.request_cs(action_node);
                            break;
                        case "f":
                            mutEx_run.node_failure(action_node);
                            break;
                    }

                }
            }

        }

        mutEx_run.printAllHist("history.txt");
        mutEx_run.terminate();
    }
}
