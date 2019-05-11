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

import it.unitn.ds1.Node;
import it.unitn.ds1.Node.SystemInitMsg;
import it.unitn.ds1.Node.SystemNodeNeighbourhoodMsg;
import it.unitn.ds1.Node.SystemWantCSMsg;
import it.unitn.ds1.Node.SystemPrintHistoryMsg;

/* TODO
assunto che no nodo crasha durante init?
assertions
metodo per fare un albero in maniera pulita (dottorando del tizio diceva a compile time, da qualche file di confi? di
akka?)
mettere if fail/recovery then etc, attualmente sti check nn ci sono
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
            if (i != N_ACTORS - 1)
                ids.add(i+1);
            // left neghbour
            if (i != 0)
                ids.add(i-1);

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

        try {
            System.out.println(">>> Wait for init to stop then ENTER to make 9 ask for CS <<<");
            System.in.read();
        } catch (IOException ioe) {
        }

        SystemWantCSMsg wantCS = new SystemWantCSMsg();
        nodes.get(9).tell(wantCS, null);


        // qua poi va fatto loop iterativo su input dell utente, per fare entrare nodi in cs, stoppare, etc.
        //
        try {
            System.out.println(">>> Wait for the chats to stop and press ENTER <<<");
            System.in.read();

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
