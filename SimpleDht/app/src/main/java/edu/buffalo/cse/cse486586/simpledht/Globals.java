package edu.buffalo.cse.cse486586.simpledht;

import android.net.Uri;

/**
 * This file contains the protocols that we define for our distributed hash table
 */
public class Globals {
    private static final String TAG = "Raman";

    public static final String JOIN_REQUEST_TO_NODE = "5554";

    public static final String REMOTE_PORT0 = "11108";
    public static final String REMOTE_PORT1 = "11112";
    public static final String REMOTE_PORT2 = "11116";
    public static final String REMOTE_PORT3 = "11120";
    public static final String REMOTE_PORT4 = "11124";

    public static final String[] ARRAY_REMOTE_PORTS = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    public static final int SERVER_PORT = 10000;
    public static final int NUM_OF_DEVICES = 5;

    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final String LOCAL_QUERY = "@";
    public static final String GLOBAL_QUERY = "*";

    public static final String NEXT_PREV_NODE_LISTENER = "next_prev_node_listener";
    public static final String TXT_PREV_NODE = "txt_prev_node";
    public static final String TXT_NEXT_NODE = "txt_next_node";

    public static final String MSG_NODE_WAIT = "msg_node_wait";

    public static final String MSG_NODE_JOIN = "msg_node_join";
    public static final String MSG_CHORD_RING_UPDATE = "msg_chord_ring_update";

    public static final String MSG_INSERT_LOOKUP = "msg_insert_lookup";
    public static final String MSG_INSERT_LOOKUP_RESPONSE = "msg_insert_lookup_response";

    public static final String MSG_QUERY_LOOKUP = "msg_query_lookup";
    public static final String MSG_QUERY_LOOKUP_RESPONSE = "msg_query_lookup_response";

    public static final String MSG_DELETE_LOOKUP = "msg_delete_lookup";
    public static final String MSG_DELETE_LOOKUP_RESPONSE = "msg_delete_lookup_response";

    public static final String MSG_GLOBAL_DUMP_REQUEST = "msg_global_dump_request";
    public static final String MSG_GLOBAL_DUMP_RESPONSE = "msg_global_dump_response";

    public static final String MSG_GLOBAL_DELETE_REQUEST = "msg_global_delete_request";
    public static final String MSG_GLOBAL_DELETE_RESPONSE = "msg_global_delete_response";

    public static final int TIMEOUT_VALUE = 3000;

    public static final Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");

    private static Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

}

