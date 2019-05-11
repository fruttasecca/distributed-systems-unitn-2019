package it.unitn.ds1;

import akka.actor.ActorRef;
import akka.actor.AbstractActor;

import java.util.*;
import java.io.Serializable;

import akka.actor.Props;

import java.util.concurrent.TimeUnit;

import akka.actor.dsl.Creators;
import scala.compat.java8.converterImpl.StepsIntRange;
import scala.concurrent.duration.FiniteDuration;

class Node extends AbstractActor {
    private final int id;    // ID of the current actor
    private List<ActorInfo> neighbours; // neighbourhood of this node (list of pairs of (actoref, int)
    private final boolean be_greedy; // should the node be greedy in adding to the queue (add itselfs on top, always)
    private ActorInfo holder; // who is the holder for this node
    private boolean using; // is the node in the CS?
    private boolean asked; // has the node asked for the PRIVILEGE token to its holder?
    private boolean in_recovery_mode; // is the node in recovery mode?
    private boolean in_failure_mode; // is the node in failure mode?
    private Deque<ActorInfo> request_queue; // Queue where to store requests incoming from neighbours.
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
    public static class NodeToNodeMsg implements Serializable {
        public final int senderId;

        public NodeToNodeMsg(final int senderId) {
            this.senderId = senderId;

        }
    }

    // Messages sent from node to node
    //###############################
    public static class InitializeMsg extends NodeToNodeMsg {
        public InitializeMsg(final int senderId) {
            super(senderId);
        }
    }

    public static class RequestMsg extends NodeToNodeMsg {
        public RequestMsg(final int senderId) {
            super(senderId);
        }
    }

    public static class PrivilegeMsg extends NodeToNodeMsg {
        public PrivilegeMsg(final int senderId) {
            super(senderId);
        }
    }

    public static class PrivilegeAndRequestMsg extends NodeToNodeMsg {
        public PrivilegeAndRequestMsg(final int senderId) {
            super(senderId);
        }
    }

    public static class RestartMsg extends NodeToNodeMsg {
        public RestartMsg(final int senderId) {
            super(senderId);
        }
    }

    public static class AdviseMsg extends NodeToNodeMsg {
        // if the node requesting the Advise message is the holder for the node sending the message.
        public final boolean you_are_my_holder;
        // if the node sending the Advise message has asked for the token to its holder.
        public final boolean asked;
        // if the node requesting the Advise message is in the request queue of the node sending the message.
        public final boolean you_asked_me;

        public AdviseMsg(final int senderId, final boolean you_are_my_holder,
                         final boolean asked, final boolean you_asked_me) {
            super(senderId);
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
        logReceivingMsg("Initialize", msg.senderId);

        holder = new ActorInfo(getSender(), msg.senderId);
        // flood the rest of the neighbours
        for (ActorInfo neigh : neighbours) {
            if (neigh.reference != holder.reference) {
                logSendingMsg("Initialize", neigh.id);
                neigh.reference.tell(new InitializeMsg(id), getSelf());
            }
        }
    }

    private void onRequestMsg(RequestMsg msg) {
        if (this.in_failure_mode){}
        else if (this.in_recovery_mode){
            logReceivingMsg("Request in recovery mode", msg.senderId);
            request_queue.add(new ActorInfo(getSender(), msg.senderId));
        }
        else{
            logReceivingMsg("Request", msg.senderId);
            request_queue.add(new ActorInfo(getSender(), msg.senderId));
            assignPrivilege();
            makeRequest();
        }
    }

    private void onPrivilegeMsg(PrivilegeMsg msg) {
        if (this.in_failure_mode){}
        else if (this.in_recovery_mode){
            logReceivingMsg("Privilege in recovery mode", msg.senderId);
            holder = new ActorInfo(getSelf(), id);
        }
        else {
            logReceivingMsg("Privilege", msg.senderId);
            holder = new ActorInfo(getSelf(), id);
            assignPrivilege();
            makeRequest();
        }
    }

    private void onPrivilegeAndRequestMsg(PrivilegeAndRequestMsg msg) {
        logReceivingMsg("PrivilegeAndRequest", msg.senderId);
        holder = new ActorInfo(getSelf(), id);
        request_queue.add(new ActorInfo(getSender(), msg.senderId));
        assignPrivilege();
        makeRequest();
    }

    private void onRestartMsg(RestartMsg msg) {
        logReceivingMsg("Restart", msg.senderId);
        boolean you_are_my_holder = getSender() == holder.reference;
        boolean you_asked_me = request_queue.contains(new ActorInfo(getSender(), msg.senderId));

        logSendingMsg("Advise", msg.senderId);
        getSender().tell(new AdviseMsg(id, you_are_my_holder, asked, you_asked_me), getSelf());
    }

    private void onAdviseMsg(AdviseMsg msg) {
        logReceivingMsg("Advise", msg.senderId);
        total_advise_msgs_received++;

        // note that msg.asked and mgs.you_asked_me are mutually exclusive
        if (msg.asked && msg.you_are_my_holder) {
            // if the other node asked us for the PRIVILEGE we enqueue it
            request_queue.add(new ActorInfo(getSender(), msg.senderId));
        } else if (!msg.you_are_my_holder) {
            /*
            if the receiving node is not the holder of the sender, then the sender is the holder
            of the receiver (this node)
             */
            holder = new ActorInfo(getSender(), msg.senderId);

            // did we ask our holder for the privilege?
            if (msg.you_asked_me)
                asked = true;
        }

        // if we received all Advise messages we can get out of the recovery phase
        // holder, asked, and the request queue have been incrementally built each time we received a msg
        if (total_advise_msgs_received == neighbours.size()) {
            in_recovery_mode = false;
            logExitRecoveryMode();
            assignPrivilege();
            makeRequest();
        }
    }

    // Methods for messages sent from a node to itself
    //###############################

    private void onSelfExitCSMsg(SelfExitCSMsg msg) {
        logReceivingMsg("ExitCS", id);
        String to_log = String.format("Node %02d exited CS", this.id);
        history.append(to_log);
        System.out.print(to_log);

        using = false;
        assignPrivilege();
        makeRequest();
    }

    private void onSelfStartRecoveryMsg(SelfStartRecoveryMsg msg) {
        logReceivingMsg("SelfStartRecoveryMsg", id);
        // we are now in recovery mode
        in_failure_mode = false;
        in_recovery_mode = true;
        using = false;

        // throughout the recovery these fields will be modified/updated incrementally each time
        // we receive ad advise msg
        asked = false;
        holder = new ActorInfo(getSelf(), id);
        request_queue = new ArrayDeque<>();
        // we will use this to decide when the recovery is over
        total_advise_msgs_received = 0;

        // ask all neighbours for Advise Msgs
        for (ActorInfo neigh : neighbours) {
            logSendingMsg("RESTART", neigh.id);
            neigh.reference.tell(new RestartMsg(id), getSelf());
        }
    }

    // Methods for messages sent from the system to a node
    //###############################

    private void onSystemNodeNeighbourhoodMsg(SystemNodeNeighbourhoodMsg msg) {
        // build all pairs of (actor reference, id)
        List<ActorInfo> neighbours = new ArrayList<>();
        Iterator<ActorRef> refIte = msg.neighbourhood.iterator();
        Iterator<Integer> idIte = msg.neighbourhood_ids.iterator();
        while (refIte.hasNext() && idIte.hasNext()) {
            neighbours.add(new ActorInfo(refIte.next(), idIte.next()));
        }
        this.neighbours = Collections.unmodifiableList(neighbours);


        String to_log = String.format("Node %02d neighbourhood: %s\n", this.id, msg.neighbourhood_ids);
        history.append(to_log);
        System.out.print(to_log);
    }

    private void onSystemInitMsg(SystemInitMsg msg) {
        logReceivingMsg("SystemInitMessage", -1);
        // i am the holder now
        holder = new ActorInfo(getSelf(), id);
        // flood neighbours

        for (ActorInfo neigh : neighbours) {
            logSendingMsg("Initialize", neigh.id);
            neigh.reference.tell(new InitializeMsg(id), getSelf());
        }
    }

    private void onSystemWantCSMsg(SystemWantCSMsg msg) {
        if (this.in_failure_mode){}
        else if (this.in_recovery_mode){
            logReceivingMsg("SystemWantCSMsg in recovery mode", -1);
            if (!queueContainsSelf())
                request_queue.add(new ActorInfo(getSelf(), id));
        }
        else {
            logReceivingMsg("SystemWantCSMsg", -1);
            if (!queueContainsSelf())
                request_queue.add(new ActorInfo(getSelf(), id));
            assignPrivilege();
            makeRequest();
        }
    }

    private void onSystemFailMsg(SystemFailMsg msg) {
        logReceivingMsg("SystemFailMsg", -1);
        // node cannot fail in recovery mode and while is in critical section by assumption
        if (!in_recovery_mode && !using) {
            in_failure_mode = true;
            // send a recovery message to yourself to simulate the end of the failure in the future
            SelfStartRecoveryMsg rec_msg = new SelfStartRecoveryMsg();
            context().system().scheduler().scheduleOnce(FAILURE_DURATION, getSelf(),
                    rec_msg, context().system().dispatcher(), null);
        }
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
        if ((holder.reference != getSelf()) && (!request_queue.isEmpty()) && (!asked)) {
            logSendingMsg("Request", holder.id);
            holder.reference.tell(new RequestMsg(id), getSelf());
            asked = true;
        }
    }

    private void assignPrivilege() {
        if ((holder.reference == getSelf()) && (!using) && (!request_queue.isEmpty())) {
            holder = request_queue.removeFirst();
            asked = false;

            // if we are the holder we can use the CS, if interested
            if (holder.reference == getSelf()) {
                using = true;

                // simulate CS duration by a message that is sent to us in the future
                context().system().scheduler().scheduleOnce(CS_DURATION, getSelf(),
                        new SelfExitCSMsg(), context().system().dispatcher(), null);

                String to_log = String.format("Node %02d entered CS", this.id);
                history.append(to_log);
                System.out.print(to_log);
            } else {
                // if we are not the new holder send the Privilege to the new one
                logSendingMsg("Privilege", holder.id);
                holder.reference.tell(new PrivilegeMsg(id), getSelf());
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

    private void logExitRecoveryMode(){
        String to_log = String.format("Node %02d exited recovery mode", this.id);
        history.append(to_log);
        System.out.print(to_log);
    }

    // utility classes
    class ActorInfo {
        public final ActorRef reference;
        public final int id;

        public ActorInfo(final ActorRef reference, final int id) {
            this.reference = reference;
            this.id = id;
        }
    }

    private boolean queueContainsSelf(){
        for(ActorInfo ai:this.request_queue){
            if (ai.id == this.id)
                return true;
        }
        return false;
    }

}
