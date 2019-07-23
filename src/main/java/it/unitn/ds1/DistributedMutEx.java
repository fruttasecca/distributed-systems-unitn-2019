package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import it.unitn.ds1.Node.SystemInitMsg;
import it.unitn.ds1.Node.SystemNodeNeighbourhoodMsg;
import it.unitn.ds1.Node.SystemWantCSMsg;
import it.unitn.ds1.Node.SystemPrintHistoryMsg;
import it.unitn.ds1.Node.SystemFailMsg;


public class DistributedMutEx {
    public static int getnActors() {
        return N_ACTORS;
    }

    final private static int N_ACTORS = 10; // number of actors

    final private ActorSystem system = ActorSystem.create("distributed_mutual_exclusion");
    List<ActorRef> nodes = new ArrayList<>();

    /**
     * actor system initialization: create N_ACTORS and "locates" them in a binary three logical network
     */
    public void init() {

        for (int i = 0; i < N_ACTORS; i++) {
            this.nodes.add(system.actorOf(Node.props(i), "Node_" + i));

        }
        this.nodes = Collections.unmodifiableList(this.nodes);

        for (int i = 0; i < N_ACTORS; i++) {
            List<Integer> ids = new ArrayList<>();

            // parent
            if (i != 0) {
                int p = (i - 1) / 2;
                ids.add(p);
            }
            // children
            // left child
            int lc = i * 2 + 1;
            if (lc < N_ACTORS) {
                ids.add(lc);
                // right child
                int rc = lc + 1;
                if (rc < N_ACTORS) {
                    ids.add(rc);
                }
            }

            List<ActorRef> neighs = new ArrayList<>();
            List<Integer> neigh_ids = new ArrayList<>();

            for (Integer id : ids) {
                neighs.add(this.nodes.get(id));
                neigh_ids.add(id);
            }

            neighs = Collections.unmodifiableList(neighs);
            neigh_ids = Collections.unmodifiableList(neigh_ids);
            SystemNodeNeighbourhoodMsg neigh_msg = new SystemNodeNeighbourhoodMsg(neighs, neigh_ids);

            this.nodes.get(i).tell(neigh_msg, null);
        }


        SystemInitMsg init = new SystemInitMsg();
        this.nodes.get(0).tell(init, null);

    }

    /**
     * actor system termination
     */
    public void terminate() {
        system.terminate();
    }

    /**
     * simulates request of a node to enter the critical section
     * @param node: node index
     */
    public void request_cs(int node) {
        SystemWantCSMsg wantCS = new SystemWantCSMsg();
        this.nodes.get(node).tell(wantCS, null);
    }


    /**
     * simulates node failure
     * @param node: node index
     */
    public void node_failure(int node) {
        SystemFailMsg fail = new SystemFailMsg();
        nodes.get(node).tell(fail, null);
    }

    /**
     * print history of selected node only in history out file
     * @param node: node index
     * @param filename: out file name
     */
    public void printHist(int node, String filename) {
        SystemPrintHistoryMsg pHist = new SystemPrintHistoryMsg(filename);
        this.nodes.get(node).tell(pHist, null);
    }

    /**
     * prints history of all nodes
     * @param filename: out file name
     */
    public void printAllHist(String filename){
        SystemPrintHistoryMsg msg = new SystemPrintHistoryMsg(filename);
                    for (ActorRef peer : nodes) {
                        peer.tell(msg, null);
                    }
    }

}


