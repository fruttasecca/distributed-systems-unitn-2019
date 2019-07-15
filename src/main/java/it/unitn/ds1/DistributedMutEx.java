package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import it.unitn.ds1.Node;
import it.unitn.ds1.Node.SystemInitMsg;
import it.unitn.ds1.Node.SystemNodeNeighbourhoodMsg;
import it.unitn.ds1.Node.SystemWantCSMsg;
import it.unitn.ds1.Node.SystemPrintHistoryMsg;
import it.unitn.ds1.Node.SystemFailMsg;

/* TODO
assunto che no nodo crasha durante init?
assertions
metodo per fare un albero in maniera pulita (dottorando del tizio diceva a compile time, da qualche file di confi? di
akka?)
mettere if fail/recovery then etc, attualmente sti check nn ci sono --> fatto
aggiunere id di sender a messaggi node to node
in init, failure e recovery settare bene tutti i campi, nn fatto per init e failure
 */

public class DistributedMutEx {
    final private static int N_ACTORS = 10; // number of actors
    static final private boolean BE_GREEDY = false;

    public static void main(String[] args) {
        // Create the 'helloakka' actor system
        final ActorSystem system = ActorSystem.create("distributed_mutual_exclusion");

        List<ActorRef> nodes = new ArrayList<>();

        for (int i = 0; i < N_ACTORS; i++) {
            nodes.add(system.actorOf(Node.props(i, BE_GREEDY), "Node_" + i));
        }
        nodes = Collections.unmodifiableList(nodes);

        // per ogni nodo aggiungeremo poi i suoi neighbours così, iterativamente
        // begin e end è solo per provare


        for (int i = 0; i < N_ACTORS; i++) {
            List<Integer> ids = new ArrayList<>();
            // right neighbour
//            if (i != N_ACTORS - 1)
//                ids.add(i+1);
//             left neghbour
//            if (i != 0)
//                ids.add(i-1);

            // parent
            if (i != 0) {
                int p = (i - 1) / 2;
                ids.add(p);
            }
            // children
            // left child
            int lc = i * 2 + 1;
            if (lc < N_ACTORS){
                ids.add(lc);
                // right child
                int rc = lc + 1;
                if (rc < N_ACTORS) {
                    ids.add(rc);
                }
            }

            List<ActorRef> neighs = new ArrayList<>();
            List<Integer> neigh_ids = new ArrayList<>();

            for (Integer id:ids){
                neighs.add(nodes.get(id));
                neigh_ids.add(id);
            }

            neighs = Collections.unmodifiableList(neighs);
            neigh_ids = Collections.unmodifiableList(neigh_ids);
            SystemNodeNeighbourhoodMsg neigh_msg = new SystemNodeNeighbourhoodMsg(neighs, neigh_ids);

            nodes.get(i).tell(neigh_msg, null);
        }



        SystemInitMsg init = new SystemInitMsg();
        nodes.get(0).tell(init, null);

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
                System.out.printf(">>> Node ids: 0 - %d <<<\n", N_ACTORS-1);
                System.out.println(">>> Or type 'e' to exit <<<\n");

                user_in = reader.readLine();

            } catch (IOException ioe) {
            }
            if (user_in.length()>0) {
                action = user_in.substring(0, 1);

                if (!action.equals("e"))
                    action_node = Integer.parseInt(user_in.substring(1, user_in.length()));
            }
            else
                valid_in = false;

            if ((!(action.equals("c")) && !(action.equals("f")) && !(action.equals("e"))) || (action_node < 0 || action_node >= N_ACTORS ))
                valid_in = false;

            if (!valid_in)
                System.out.println(">>> Not valid action code <<<");
            else {
                switch (action){
                    case "e":
                        exit = true;
                        break;
                    case "c":
                        SystemWantCSMsg wantCS = new SystemWantCSMsg();
                        nodes.get(action_node).tell(wantCS, null);
                        break;
                    case "f":
                        SystemFailMsg fail = new SystemFailMsg();
                        nodes.get(action_node).tell(fail, null);
                        break;
                }

            }

        }
        try {

            SystemPrintHistoryMsg msg = new SystemPrintHistoryMsg();
            for (ActorRef peer : nodes) {
                peer.tell(msg, null);
            }
            System.out.println(">>> Press ENTER to exit <<<");
            System.in.read();
        } catch (IOException ioe) {
        }

        system.terminate();
    }
}
