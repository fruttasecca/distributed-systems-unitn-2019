package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;

import java.util.*;
import java.io.Serializable;

import akka.actor.Props;

import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

class Node extends AbstractActor {
    private final int id;    // ID of the current actor
    private List<ActorRef> neighbours; // neighbourhood of this node
    private final boolean be_greedy; // should the node be greedy in adding to the queue (add itselfs on top, always)
    private ActorRef holder; // who is the holder for this node
    private boolean using; // is the node in the CS?
    private boolean asked; // has the node asked for the PRIVILEGE token to its holder?
    private boolean in_recovery_mode; // is the node in recovery mode?
    private boolean in_failure_mode; // is the node in failure mode?
    private Deque<ActorRef> request_queue; // Queue where to store requests incoming from neighbours.
    private int total_advise_msgs_received; // amount of advise msgs received during this recovery

    // durations
    private final FiniteDuration FAILURE_DURATION = new FiniteDuration(5, TimeUnit.SECONDS);
    private final FiniteDuration CS_DURATION = new FiniteDuration(5, TimeUnit.SECONDS);
    // logging
    private StringBuffer history;


    /* -- Actor constructor --------------------------------------------------- */
    public Node(final int id, final boolean be_greedy) {
        this.id = id;
        this.neighbours = null;
        this.be_greedy = be_greedy;
        this.holder = null;
        this.using = false;
        this.asked = false;
        this.in_recovery_mode = false;
        this.in_failure_mode = false;
        this.request_queue = new ArrayDeque<>();
        this.total_advise_msgs_received = 0;
        this.history = new StringBuffer();

        String to_log = String.format("Node %02d created, greedy: %b\n", this.id, this.be_greedy);
        history.append(to_log);
        System.out.print(to_log);
    }

    static public Props props(final int id, final boolean be_greedy) {
        return Props.create(Node.class, () -> new Node(id, be_greedy));
    }

    // Here we define the mapping between the received message types
    // and our actor methods
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // messages sent from node to node
                .match(InitializeMsg.class, this::onInitializeMsg)
                .match(RequestMsg.class, this::onRequestMsg)
                .match(PrivilegeMsg.class, this::onPrivilegeMsg)
                .match(PrivilegeAndRequestMsg.class, this::onPrivilegeAndRequestMsg)
                .match(RestartMsg.class, this::onRestartMsg)
                .match(AdviseMsg.class, this::onAdviseMsg)

                // messages sent from a node to itself, to trigger/simulate the exit from CS or the end of a failure
                .match(SelfExitCSMsg.class, this::onSelfExitCSMsg)
                .match(SelfStartRecoveryMsg.class, this::onSelfStartRecoveryMsg)

                // system messages, sent from "outside" (from the main loop)
                .match(SystemNodeNeighbourhoodMsg.class, this::onSystemNodeNeighbourhoodMsg)
                .match(SystemInitMsg.class, this::onSystemInitMsg)
                .match(SystemWantCSMsg.class, this::onSystemWantCSMsg)
                .match(SystemFailMsg.class, this::onSystemFailMsg)
                .match(SystemPrintHistoryMsg.class, this::onSystemPrintHistoryMsg)
                .build();
    }

    /*
    ###############################
    Message Classes
    ###############################
    */

    // Messages sent from node to node
    //###############################
    public static class InitializeMsg implements Serializable {
    }

    public static class RequestMsg implements Serializable {
    }

    public static class PrivilegeMsg implements Serializable {
    }

    public static class PrivilegeAndRequestMsg implements Serializable {
    }

    public static class RestartMsg implements Serializable {
    }

    public static class AdviseMsg implements Serializable {
        // if the node requesting the Advise message is the holder for the node sending the message.
        public final boolean you_are_my_holder;
        // if the node sending the Advise message has asked for the token to its holder.
        public final boolean asked;
        // if the node requesting the Advise message is in the request queue of the node sending the message.
        public final boolean you_asked_me;

        public AdviseMsg(boolean you_are_my_holder, boolean asked, boolean you_asked_me) {
            this.you_are_my_holder = you_are_my_holder;
            this.asked = asked;
            this.you_asked_me = you_asked_me;
        }
    }

    // Messages sent from a node to itself
    //###############################

    // Msg to be sent to itself to exit the CS
    public static class SelfExitCSMsg implements Serializable {
    }

    // Msg to be sent to itself to exit the CS
    public static class SelfStartRecoveryMsg implements Serializable {
    }

    // Messages sent from the system (main loop) to a node
    //###############################

    // we will use this to tell a node who are its neighbours
    public static class SystemNodeNeighbourhoodMsg implements Serializable {
        public final List<ActorRef> neighbourhood;
        public final List<Integer> neighbourhood_ids;

        public SystemNodeNeighbourhoodMsg(final List<ActorRef> neighbourhood, final List<Integer> neighbourhood_ids) {
            this.neighbourhood = neighbourhood;
            this.neighbourhood_ids = neighbourhood_ids;
        }
    }

    // we will use this to tell a node he is the first holder of the token,
    // the node will follow by sending the InitializeMsg to all its neighbours,
    // effectively initializing the algorithm
    public static class SystemInitMsg implements Serializable {
    }

    // we will use this to ask a node to becoming willing to enter the CS,
    // thus sending a RequestMsg sooner or later
    public static class SystemWantCSMsg implements Serializable {
    }
    // we will use this to ask a node to becoming willing to enter the CS,

    // we will use this to set a node in fail mode, after a certain time
    // it will be back to normal and start its recovery phase
    public static class SystemFailMsg implements Serializable {
    }

    // we will use this to ask a node to print its history
    public static class SystemPrintHistoryMsg implements Serializable {
    }

    /*
    ###########################
    Methods related to the receiving of a message.
    ##########################
    */

    // Methods for messages sent from node to node
    //###############################

    private void onInitializeMsg(InitializeMsg msg) {
        logReceivingMsg("Initialize", -1);

        holder = getSender();
        // flood the rest of the neighbours
        for (ActorRef neigh : neighbours) {
            if (neigh != holder) {
                logSendingMsg("Initialize", -1);
                neigh.tell(new InitializeMsg(), getSelf());
            }
        }
    }

    private void onRequestMsg(RequestMsg msg) {
        logReceivingMsg("Request", -1);
        request_queue.add(getSender());
        assignPrivilege();
        makeRequest();
    }

    private void onPrivilegeMsg(PrivilegeMsg msg) {
        logReceivingMsg("Privilege", -1);
        holder = getSelf();
        assignPrivilege();
        makeRequest();
    }

    private void onPrivilegeAndRequestMsg(PrivilegeAndRequestMsg msg) {
        logReceivingMsg("PrivilegeAndRequest", -1);
        holder = getSelf();
        request_queue.add(getSender());
        assignPrivilege();
        makeRequest();
    }

    private void onRestartMsg(RestartMsg msg) {
        logReceivingMsg("Restart", -1);
        boolean you_are_my_holder = getSender() == holder;
        boolean you_asked_me = request_queue.contains(getSender());

        logSendingMsg("Advise", -1);
        getSender().tell(new AdviseMsg(you_are_my_holder, asked, you_asked_me), getSelf());
    }

    private void onAdviseMsg(AdviseMsg msg) {
        logReceivingMsg("Advise", -1);
        total_advise_msgs_received++;

        // note that msg.asked and mgs.you_asked_me are mutually exclusive
        if (msg.asked) {
            // if the other node asked us for the PRIVILEGE we enqueue it
            request_queue.add(getSender());
        } else if (!msg.you_are_my_holder) {
            /*
            if the receiving node is not the holder of the sender, then the sender is the holder
            of the receiver (this node)
             */
            holder = getSender();

            // did we ask our holder for the privilege?
            if (msg.you_asked_me)
                asked = true;
        }

        // if we received all Advise messages we can get out of the recovery phase
        // holder, asked, and the request queue have been incrementally built each time we received a msg
        if (total_advise_msgs_received == neighbours.size()) {
            in_recovery_mode = false;
            assignPrivilege();
            makeRequest();
        }
    }

    // Methods for messages sent from a node to itself
    //###############################

    private void onSelfExitCSMsg(SelfExitCSMsg msg) {
        logReceivingMsg("ExitCS", -1);
        String to_log = String.format("Node %02d exited CS", this.id);
        history.append(to_log);
        System.out.print(to_log);

        using = false;
        assignPrivilege();
        makeRequest();
    }

    private void onSelfStartRecoveryMsg(SelfStartRecoveryMsg msg) {
        logReceivingMsg("SelfStartRecoveryMsg", -1);
        // we are now in recovery mode
        in_failure_mode = false;
        in_recovery_mode = true;
        using = false;

        // throughout the recovery these fields will be modified/updated incrementally each time
        // we receive ad advise msg
        asked = false;
        holder = getSelf();
        request_queue = new ArrayDeque<>();
        // we will use this to decide when the recovery is over
        total_advise_msgs_received = 0;

        // ask all neighbours for Advise Msgs
        for (ActorRef neigh : neighbours) {
            logSendingMsg("RESTART", -1);
            neigh.tell(new RestartMsg(), getSelf());
        }
    }

    // Methods for messages sent from the system to a node
    //###############################

    private void onSystemNodeNeighbourhoodMsg(SystemNodeNeighbourhoodMsg msg) {
        this.neighbours = msg.neighbourhood;

        String to_log = String.format("Node %02d neighbourhood: %s\n", this.id, msg.neighbourhood_ids);
        history.append(to_log);
        System.out.print(to_log);
    }

    private void onSystemInitMsg(SystemInitMsg msg) {
        logReceivingMsg("SystemInitMessage", -1);
        // i am the holder now
        holder = getSelf();
        // flood neighbours
        for (ActorRef neigh : neighbours) {
            logSendingMsg("Initialize", -1);
            neigh.tell(new InitializeMsg(), getSelf());
        }
    }

    private void onSystemWantCSMsg(SystemWantCSMsg msg) {
        logReceivingMsg("SystemWantCSMsg", -1);
        request_queue.add(getSelf());
        assignPrivilege();
        makeRequest();
    }

    private void onSystemFailMsg(SystemFailMsg msg) {
        logReceivingMsg("SystemFailMsg", -1);
        in_failure_mode = true;
        // send a recovery message to yourself to simulate the end of the failure in the future
        SelfStartRecoveryMsg rec_msg = new SelfStartRecoveryMsg();
        context().system().scheduler().scheduleOnce(FAILURE_DURATION, getSelf(),
                rec_msg, context().system().dispatcher(), null);
    }

    private void onSystemPrintHistoryMsg(SystemPrintHistoryMsg msg) {
        System.out.printf("%02d: %s\n", this.id, history);
    }

    /*
    ###########################
    Methods not directly related to the receiving of a message.
    ##########################
     */

    private void makeRequest() {
        if ((holder != getSelf()) && (!request_queue.isEmpty()) && (!asked)) {
            logSendingMsg("Request", -1);
            holder.tell(new RequestMsg(), getSelf());
            asked = true;
        }
    }

    private void assignPrivilege() {
        if ((holder == getSelf()) && (!using) && (!request_queue.isEmpty())) {
            holder = request_queue.removeFirst();
            asked = false;

            // if we are the holder we can use the CS, if interested
            if (holder == getSelf()) {
                using = true;

                // simulate CS duration by a message that is sent to us in the future
                context().system().scheduler().scheduleOnce(CS_DURATION, getSelf(),
                        new SelfExitCSMsg(), context().system().dispatcher(), null);

                String to_log = String.format("Node %02d entered CS", this.id);
                history.append(to_log);
                System.out.print(to_log);
            } else {
                // if we are not the new holder send the Privilege to the new one
                logSendingMsg("Privilege", -1);
                holder.tell(new PrivilegeMsg(), getSelf());
            }
        }
    }

    private void logReceivingMsg(final String msg_type, final int from_id) {
        String to_log = String.format("Node %02d received %s msg from node %02d\n", this.id, msg_type, from_id);
        history.append(to_log);
        System.out.print(to_log);
    }


    private void logSendingMsg(final String msg_type, final int to_id) {
        String to_log = String.format("Node %02d sent %s msg to node %02d\n", this.id, msg_type, to_id);
        history.append(to_log);
        System.out.print(to_log);
    }

}
