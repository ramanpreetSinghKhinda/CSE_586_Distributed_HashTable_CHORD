package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Handler;
import android.os.Message;
import android.support.v4.content.LocalBroadcastManager;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getName();

    /**
     * Database specific constant declarations
     */
    private SQLiteDatabase db;
    private static final int DATABASE_VERSION = 1;
    private static final String DATABASE_NAME = "SimpleDHT";
    private static final String TABLE_NAME = "PA_3";
    private static final String COL_KEY_FIELD = "key";
    private static final String COL_VALUE_FIELD = "value";

    // A string that defines the SQL statement for creating a table
    private static final String CREATE_DB_TABLE = " CREATE TABLE " +
            TABLE_NAME +
            " (" +
            COL_KEY_FIELD + " TEXT NOT NULL, " +
            COL_VALUE_FIELD + " TEXT NOT NULL);";


    private String MY_PORT;
    private String MY_EMULATOR_NODE;

    private ConcurrentHashMap<RamanKey, Lookup> chordRingMap = new ConcurrentHashMap<RamanKey, Lookup>();

    // This map acts as responder for the inserts
    private ConcurrentHashMap<String, RamanMessage> insertRequestMap = new ConcurrentHashMap<String, RamanMessage>();

    // This map acts as both requester as well as responder for the queries
    private ConcurrentHashMap<String, RamanMessage> queryRequestResponseMap = new ConcurrentHashMap<String, RamanMessage>();

    // This map acts as both requester as well as responder for the queries
    private ConcurrentHashMap<String, RamanMessage> deleteRequestResponseMap = new ConcurrentHashMap<String, RamanMessage>();

    // This map acts as responder for the global queries/delete requests
    private ConcurrentHashMap<RamanKey, ArrayList<RamanMessage>> globalQueryResponseMap = new ConcurrentHashMap<RamanKey, ArrayList<RamanMessage>>();
    private ConcurrentHashMap<RamanKey, Integer> globalDeleteResponseMap = new ConcurrentHashMap<RamanKey, Integer>();

    private CountDownLatch nodeJoinCountDownLatch;
    private CountDownLatch insertCountDownLatch;
    private CountDownLatch queryCountDownLatch;
    private CountDownLatch deleteCountDownLatch;
    private CountDownLatch globalCountDownLatch;

    private long initialTime;
    private long finalTime;
    // List of Modified Nodes on the chord Ring
    private CopyOnWriteArrayList<RamanKey> modifiedNodes = new CopyOnWriteArrayList<RamanKey>();

    // Stores my lookup nodes
    private Lookup myLookup;

    private MatrixCursor globalDumpCursor = null;
    private AtomicInteger singleNode = new AtomicInteger(0);

    private AtomicInteger response = new AtomicInteger(0);

    /**
     * Helper class that actually creates and manages the provider's underlying data repository.
     */
    private static class RamanDatabaseHelper extends SQLiteOpenHelper {
        /**
         * Instantiates an open helper for the provider's SQLite data repository
         * Do not do database creation and upgrade here.
         */
        RamanDatabaseHelper(Context context) {
            super(context, DATABASE_NAME, null, DATABASE_VERSION);
        }

        /**
         * Creates the data repository.
         * This is called when the provider attempts to open the repository
         * and SQLite reports that it doesn't exist.
         */
        @Override
        public void onCreate(SQLiteDatabase db) {
            // Creates the Database table
            db.execSQL(CREATE_DB_TABLE);
            Log.v(TAG, "Raman Database Table created");
        }

        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.v(TAG, "Raman upgrading Database from VERSION : " + oldVersion + " to VERSION : " + newVersion);
            db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
            onCreate(db);
        }
    }

    @Override
    public boolean onCreate() {
        Log.v(TAG, "***** Raman inside onCreate() START *****");

        Context context = getContext();

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        MY_EMULATOR_NODE = String.valueOf((Integer.parseInt(portStr)));
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));
        boolean serverStarted = true;
        try {
            //Create a server socket as well as a thread (AsyncTask) that listens on the server port.
            ServerSocket serverSocket = new ServerSocket(Globals.SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            serverStarted = false;
            Log.v(TAG, "Can't create a ServerSocket");
        } finally {
            if (serverStarted) {
                notifyChordRing(new RamanMessage(MY_EMULATOR_NODE, MY_EMULATOR_NODE, Globals.JOIN_REQUEST_TO_NODE, Globals.MSG_NODE_JOIN));

                if (!MY_EMULATOR_NODE.equalsIgnoreCase(Globals.JOIN_REQUEST_TO_NODE)) {
                    initialTime = System.currentTimeMillis();
                    notifyChordRing(new RamanMessage(MY_EMULATOR_NODE, MY_EMULATOR_NODE, MY_EMULATOR_NODE, Globals.MSG_NODE_WAIT));

                    nodeJoinCountDownLatch = new CountDownLatch(1);

                    try {
                        Log.v(TAG, "Waiting for response for successful Join. Main Thread Paused...");
                        nodeJoinCountDownLatch.await();  //main thread is waiting on CountDownLatch to finish
                        Log.v(TAG, "Got response for the join request. Application is starting now !!!");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        /**
         * Creates a new helper object. This method always returns quickly.
         * Notice that the database itself isn't created or opened
         * until SQLiteOpenHelper.getWritableDatabase is called
         */
        RamanDatabaseHelper dbHelper = new RamanDatabaseHelper(context);

        /**
         * Create a write able database which will trigger its
         * creation if it doesn't already exist.
         */
        db = dbHelper.getWritableDatabase();

        Log.v(TAG, "***** Raman inside onCreate() END. The created db is : " + db + " *****");
        return (db == null) ? false : true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        Log.v(TAG, "***** Raman inside insert() START. Uri : " + uri + " , ContentValues : " + values.toString() + " *****");

        Uri contentUri = uri;
        String hashKey, hashPrevNode, hashMyNode;
        boolean canInsert = false;

        final String originNode = (null == insertRequestMap.get(values.getAsString(COL_KEY_FIELD))) ? MY_EMULATOR_NODE : insertRequestMap.get(values.getAsString(COL_KEY_FIELD)).getOriginNode();

        try {
            hashKey = genHash(values.getAsString(COL_KEY_FIELD));
            hashMyNode = genHash(MY_EMULATOR_NODE);
            hashPrevNode = genHash(myLookup.getPrevNode());

            Log.v(TAG, "hashKey : " + hashKey);
            Log.v(TAG, "hashMyNode : " + hashMyNode);
            Log.v(TAG, "hashPrevNode : " + hashPrevNode);

            if (hashMyNode.compareTo(hashPrevNode) < 0
                    && ((hashKey.compareTo(hashPrevNode) > 0 && hashKey.compareTo(hashMyNode) > 0) ||
                    (hashKey.compareTo(hashPrevNode) < 0 && hashKey.compareTo(hashMyNode) < 0))) {
                canInsert = true;
                Log.v(TAG, "Can run insert() request on " + MY_EMULATOR_NODE + " because this key is either smallest or largest hashed value and my node is the first node");

            } else if (1 == singleNode.get()) {
                canInsert = true;
                Log.v(TAG, "Can run insert() request on " + MY_EMULATOR_NODE + " because there is only 1 node in the system");

            } else if (hashKey.compareTo(hashPrevNode) > 0 && hashKey.compareTo(hashMyNode) <= 0) {
                canInsert = true;
                Log.v(TAG, "Can run insert() request on " + MY_EMULATOR_NODE + " because the condition satisfied");

            } else {
                canInsert = false;

                RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, myLookup.getNextNode(), Globals.MSG_INSERT_LOOKUP);
                ramanMessage.setKey(values.getAsString(COL_KEY_FIELD));
                ramanMessage.setValue(values.getAsString(COL_VALUE_FIELD));

                Log.v(TAG, "Sending insert() request to next node");
                notifyChordRing(ramanMessage);

                if (MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                    // It means that I am the requester for this insert request, so pause my application, otherwise don't pause other node application
                    insertCountDownLatch = new CountDownLatch(1);

                    try {
                        Log.v(TAG, "Waiting for response for successful insert. Main Thread Paused...");
                        insertCountDownLatch.await();  //main thread is waiting on CountDownLatch to finish
                        Log.v(TAG, "Got response for the insert query. Application is starting now !!!");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        if (canInsert) {
            // checking if a value with specified key already exists
            SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();
            queryBuilder.setTables(TABLE_NAME);
            Cursor cursor = queryBuilder.query(db, null, COL_KEY_FIELD + "=?", new String[]{values.getAsString(COL_KEY_FIELD)}, null, null, null);

            if (cursor.moveToFirst()) {
                Log.v(TAG, "The specific KEY : " + values.getAsString(COL_KEY_FIELD) + " already exists hence only UPDATE the VALUE");

                db.update(TABLE_NAME, values, COL_KEY_FIELD + "=?", new String[]{values.getAsString(COL_KEY_FIELD)});
            } else {
                Log.v(TAG, "Inserting new KEY-VALUE pair");

                /**
                 * Add a new record
                 * @return the row ID of the newly inserted row, or -1 if an error occurred
                 */
                long rowId = db.insert(TABLE_NAME, "", values);

                /**
                 * If record is added successfully
                 */
                if (rowId > 0) {
                    /**
                     * Appends the given ID to the end of the path
                     * This is used to access a particular row in case
                     */
                    contentUri = ContentUris.withAppendedId(uri, rowId);
                }
            }
        }

        if (canInsert && !MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
            Log.v(TAG, "***** Raman inside insert() END. Query by Node " + originNode + " runs Successfully at Node " + MY_EMULATOR_NODE + " *****");

            RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, originNode, Globals.MSG_INSERT_LOOKUP_RESPONSE);
            Log.v(TAG, "Sending insert() successful response to the origin node " + originNode);
            notifyChordRing(ramanMessage);

        } else if (MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
            Log.v(TAG, "***** Raman inside insert() END. Query by Node " + originNode + "  Successful at " + MY_EMULATOR_NODE + "*****");

        } else {
            Log.v(TAG, "***** Raman inside insert() END. Query NOT Successful at Node " + MY_EMULATOR_NODE + " *****");
        }

        return contentUri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.v(TAG, "***** Raman inside query() START. Uri : " + uri + " , Selection : " + selection + " *****");

        SQLiteQueryBuilder queryBuilder = new SQLiteQueryBuilder();

        // Sets the list of tables to query
        queryBuilder.setTables(TABLE_NAME);
        Cursor cursor = null;

        if (null != selection) {
            if (selection.equalsIgnoreCase("@")) {
                // Return all <key, value> pairs stored in local partition of the node
                Log.v(TAG, "Running @ query");
                cursor = queryBuilder.query(db, projection, null, null, null, null, null);

            } else if (selection.equalsIgnoreCase("*")) {
                //  Return all <key, value> pairs stored in your entire DHT

                if (1 == singleNode.get()) {
                    Log.v(TAG, "Can run query() on " + MY_EMULATOR_NODE + " because there is only 1 node in the system");
                    cursor = queryBuilder.query(db, projection, null, null, null, null, null);
                    return cursor;
                }

                RamanKey ramanOriginKey = null;

                try {
                    Log.v(TAG, "Running * query");

                    String hashOriginNode;
                    String originNode;

                    // Hack of sending origin node in sort order
                    originNode = (null == sortOrder) ? MY_EMULATOR_NODE : sortOrder;

                    hashOriginNode = genHash(originNode);
                    Log.v(TAG, "hashOriginNode : " + hashOriginNode);

                    ramanOriginKey = new RamanKey(hashOriginNode, originNode);

                    // Double checking of hack, because request from grader may also include non null sort order
                    if (!MY_EMULATOR_NODE.equalsIgnoreCase(originNode) && null == globalQueryResponseMap.get(ramanOriginKey)) {
                        // It means that selection of origin node was wrong, revert it
                        originNode = MY_EMULATOR_NODE;
                        hashOriginNode = genHash(originNode);
                        Log.v(TAG, "New hashOriginNode : " + hashOriginNode);
                        ramanOriginKey = new RamanKey(hashOriginNode, originNode);
                    }

                    final RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, myLookup.getNextNode(), Globals.MSG_GLOBAL_DUMP_REQUEST);

                    if (!MY_EMULATOR_NODE.equalsIgnoreCase(originNode) || MY_EMULATOR_NODE.equalsIgnoreCase(originNode) && null == globalQueryResponseMap.get(ramanOriginKey)) {
                        // If I got the request from my prev node than it means this should be the last message and I should not send new dump request again
                        Log.v(TAG, "Sending global dump request to my next node");
                        notifyChordRing(ramanMessage);
                    }

                    if (MY_EMULATOR_NODE.equalsIgnoreCase(originNode) && null == globalQueryResponseMap.get(ramanOriginKey)) {
                        // It means that I am the requester for this query request, so pause my application, otherwise don't pause other node application
                        ArrayList<RamanMessage> list = new ArrayList<RamanMessage>();
                        list.add(ramanMessage);

                        globalQueryResponseMap.put(ramanOriginKey, list);
                        String[] columns = new String[]{COL_KEY_FIELD, COL_VALUE_FIELD};
                        globalDumpCursor = new MatrixCursor(columns);

                        globalCountDownLatch = new CountDownLatch(1);

                        try {
                            Log.v(TAG, "Waiting for response for successful query. Main Thread Paused...");
                            globalCountDownLatch.await();  //main thread is waiting on CountDownLatch to finish
                            Log.v(TAG, "Got response for the query. Application is starting now !!!");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Log.v(TAG, "Response received for global query lookup : ");

                        JSONParser parser = new JSONParser();

                        for (int i = 0; i < globalQueryResponseMap.get(ramanOriginKey).size(); i++) {
                            try {
                                String jsonResult = globalQueryResponseMap.get(ramanOriginKey).get(i).getJsonString();
                                Object obj = parser.parse(jsonResult);
                                JSONObject jsonObject = (JSONObject) obj;

                                Iterator<String> itr = jsonObject.keySet().iterator();

                                while (itr.hasNext()) {
                                    String key = itr.next();
                                    String value = (String) jsonObject.get(key);

                                    globalDumpCursor.addRow(new Object[]{key, value});
                                }
                            } catch (NullPointerException e) {
                                e.printStackTrace();
                                // It means someone have sent empty response, despite of efficient n/w traffic handling protocol
                                // But.... This happened due to special permission of origin node. So no worries
                            } catch (ParseException e) {
                                e.printStackTrace();
                                // It means someone have sent empty response, despite of efficient n/w traffic handling protocol
                                // But.... This happened due to special permission of origin node. So no worries
                            }
                        }

                        if (null != ramanOriginKey) {
                            globalQueryResponseMap.remove(ramanOriginKey);
                        }

                        Log.v(TAG, "***** Raman inside query END. Cursor returned is : " + globalDumpCursor + " *****");
                        return globalDumpCursor;

                    } else {
                        // returning my cursor response for the global dump request to the origin node
                        cursor = queryBuilder.query(db, projection, null, null, null, null, null);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            } else {
                final String originNode = (null == queryRequestResponseMap.get(selection)) ? MY_EMULATOR_NODE : (queryRequestResponseMap.get(selection)).getOriginNode();

                String hashKey, hashPrevNode, hashMyNode;
                boolean canQuery = false;

                try {
                    hashKey = genHash(selection);
                    hashMyNode = genHash(MY_EMULATOR_NODE);
                    hashPrevNode = genHash(myLookup.getPrevNode());

                    Log.v(TAG, "hashKey : " + hashKey);
                    Log.v(TAG, "hashMyNode : " + hashMyNode);
                    Log.v(TAG, "hashPrevNode : " + hashPrevNode);

                    if (hashMyNode.compareTo(hashPrevNode) < 0
                            && ((hashKey.compareTo(hashPrevNode) > 0 && hashKey.compareTo(hashMyNode) > 0) ||
                            (hashKey.compareTo(hashPrevNode) < 0 && hashKey.compareTo(hashMyNode) < 0))) {
                        canQuery = true;
                        Log.v(TAG, "Can run query() request on " + MY_EMULATOR_NODE + " because this key is either smallest or largest hashed value and my node is the first node");

                    } else if (1 == singleNode.get()) {
                        canQuery = true;
                        Log.v(TAG, "Can run query() request on " + MY_EMULATOR_NODE + " because there is only 1 node in the system");

                    } else if (hashKey.compareTo(hashPrevNode) > 0 && hashKey.compareTo(hashMyNode) <= 0) {
                        canQuery = true;
                        Log.v(TAG, "Can run query() request on " + MY_EMULATOR_NODE + " because the condition satisfied");

                    } else {
                        canQuery = false;

                        RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, myLookup.getNextNode(), Globals.MSG_QUERY_LOOKUP);
                        ramanMessage.setKey(selection);

                        Log.v(TAG, "Sending query() request to next node");
                        notifyChordRing(ramanMessage);

                        if (MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                            queryRequestResponseMap.put(selection, ramanMessage);

                            // It means that I am the requester for this query request, so pause my application, otherwise don't pause other node application
                            queryCountDownLatch = new CountDownLatch(1);

                            try {

                                Log.v(TAG, "Waiting for response for successful query. Main Thread Paused...");
                                queryCountDownLatch.await();  //main thread is waiting on CountDownLatch to finish
                                Log.v(TAG, "Got response for the query. Application is starting now !!!");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            Log.v(TAG, "Response received for query lookup : " + queryRequestResponseMap.get(selection).getJsonString());

                            JSONParser parser = new JSONParser();
                            Object obj = parser.parse(queryRequestResponseMap.get(selection).getJsonString());
                            JSONObject jsonObject = (JSONObject) obj;

                            String[] columns = new String[]{COL_KEY_FIELD, COL_VALUE_FIELD};
                            MatrixCursor matrixCursor = new MatrixCursor(columns);

                            Iterator<String> itr = jsonObject.keySet().iterator();

                            while (itr.hasNext()) {
                                String key = itr.next();
                                String value = (String) jsonObject.get(key);

                                matrixCursor.addRow(new Object[]{key, value});
                            }

                            Log.v(TAG, "***** Raman inside query END. Cursor returned is : " + matrixCursor + " *****");
                            return matrixCursor;
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (ParseException e) {
                    e.printStackTrace();
                } finally {
                    queryRequestResponseMap.remove(selection);
                }

                if (canQuery) {
                    //  Return a particular <key, value> pair represented by the selection
                    cursor = queryBuilder.query(db, projection, COL_KEY_FIELD + "=?", new String[]{selection}, null, null, sortOrder);
                }
            }
        } else {
            Log.v(TAG, "Selection is null");
        }

        Log.v(TAG, "***** Raman inside query END. Cursor returned is : " + cursor + " *****");
        return cursor;
    }


    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v(TAG, "***** Raman inside delete() START. Uri : " + uri + " , Selection : " + selection + " *****");

        /**
         * @return the number of rows affected if a whereClause is passed in, 0
         *         otherwise. To remove all rows and get a count pass "1" as the
         *         whereClause.
         */
        int rows = -1;

        if (null != selection) {
            if (selection.equalsIgnoreCase("@")) {
                // Delete all <key, value> pairs stored in local partition of the node
                Log.v(TAG, "Running @ delete query");

                rows = db.delete(TABLE_NAME, null, null);
            } else if (selection.equalsIgnoreCase("*")) {
                //  Delete all <key, value> pairs stored in your entire DHT
                Log.v(TAG, "Running * delete query");

                if (1 == singleNode.get()) {
                    Log.v(TAG, "Can run delete() on " + MY_EMULATOR_NODE + " because there is only 1 node in the system");
                    rows = db.delete(TABLE_NAME, null, null);
                    return rows;
                }

                RamanKey ramanOriginKey = null;

                try {
                    String hashOriginNode;
                    String originNode;

                    // Hack of sending origin node in sort order
                    originNode = (null == selectionArgs) ? MY_EMULATOR_NODE : selectionArgs[0];

                    hashOriginNode = genHash(originNode);
                    Log.v(TAG, "hashOriginNode : " + hashOriginNode);

                    ramanOriginKey = new RamanKey(hashOriginNode, originNode);

                    // Double checking of hack, because request from grader may also include non null sort order
                    if (!MY_EMULATOR_NODE.equalsIgnoreCase(originNode) && null == globalDeleteResponseMap.get(ramanOriginKey)) {
                        // It means that selection of origin node was wrong, revert it
                        originNode = MY_EMULATOR_NODE;
                        hashOriginNode = genHash(originNode);
                        Log.v(TAG, "New hashOriginNode : " + hashOriginNode);
                        ramanOriginKey = new RamanKey(hashOriginNode, originNode);
                    }

                    final RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, myLookup.getNextNode(), Globals.MSG_GLOBAL_DELETE_REQUEST);

                    if (!MY_EMULATOR_NODE.equalsIgnoreCase(originNode) || MY_EMULATOR_NODE.equalsIgnoreCase(originNode) && null == globalDeleteResponseMap.get(ramanOriginKey)) {
                        // If I got the request from my prev node than it means this should be the last message and I should not send new dump request again
                        Log.v(TAG, "Sending global delete request to my next node");
                        notifyChordRing(ramanMessage);
                    }
                    if (MY_EMULATOR_NODE.equalsIgnoreCase(originNode) && null == globalDeleteResponseMap.get(ramanOriginKey)) {
                        // It means that I am the requester for this query request, so pause my application, otherwise don't pause other node application
                        globalDeleteResponseMap.put(ramanOriginKey, 0);
                        globalCountDownLatch = new CountDownLatch(1);

                        try {
                            Log.v(TAG, "Waiting for response for successful delete. Main Thread Paused...");
                            globalCountDownLatch.await();  //main thread is waiting on CountDownLatch to finish
                            Log.v(TAG, "Got response for the delete. Application is starting now !!!");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        Log.v(TAG, "Response received for global delete request");

                        rows = globalDeleteResponseMap.get(ramanOriginKey);

                        if (null != ramanOriginKey) {
                            globalDeleteResponseMap.remove(ramanOriginKey);
                        }
                    } else {
                        // delete my db and send response for the global delete request to the origin node
                        rows = db.delete(TABLE_NAME, null, null);
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
            } else {
                final String originNode = (null == deleteRequestResponseMap.get(selection)) ? MY_EMULATOR_NODE : (deleteRequestResponseMap.get(selection)).getOriginNode();

                String hashKey, hashPrevNode, hashMyNode, hashOriginNode;
                boolean canDelete = false;

                try {
                    hashKey = genHash(selection);
                    hashMyNode = genHash(MY_EMULATOR_NODE);
                    hashPrevNode = genHash(myLookup.getPrevNode());
                    hashOriginNode = genHash(originNode);

                    Log.v(TAG, "hashKey : " + hashKey);
                    Log.v(TAG, "hashMyNode : " + hashMyNode);
                    Log.v(TAG, "hashPrevNode : " + hashPrevNode);

                    if (hashMyNode.compareTo(hashPrevNode) < 0
                            && ((hashKey.compareTo(hashPrevNode) > 0 && hashKey.compareTo(hashMyNode) > 0) ||
                            (hashKey.compareTo(hashPrevNode) < 0 && hashKey.compareTo(hashMyNode) < 0))) {
                        canDelete = true;
                        Log.v(TAG, "Can run delete() request on " + MY_EMULATOR_NODE + " because this key is either smallest or largest hashed value and my node is the first node");

                    } else if (1 == singleNode.get()) {
                        canDelete = true;
                        Log.v(TAG, "Can run delete() request on " + MY_EMULATOR_NODE + " because there is only 1 node in the system");

                    } else if (hashKey.compareTo(hashPrevNode) > 0 && hashKey.compareTo(hashMyNode) <= 0) {
                        canDelete = true;
                        Log.v(TAG, "Can run delete() request on " + MY_EMULATOR_NODE + " because the condition satisfied");

                    } else {
                        canDelete = false;

                        RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, myLookup.getNextNode(), Globals.MSG_DELETE_LOOKUP);
                        ramanMessage.setKey(selection);
                        ramanMessage.setRowsDeleted(-1);

                        Log.v(TAG, "Sending delete() request to next node");
                        notifyChordRing(ramanMessage);

                        if (MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                            deleteRequestResponseMap.put(selection, ramanMessage);

                            // It means that I am the requester for this delete request, so pause my application, otherwise don't pause other node application
                            deleteCountDownLatch = new CountDownLatch(1);

                            try {

                                Log.v(TAG, "Waiting for response for successful delete. Main Thread Paused...");
                                deleteCountDownLatch.await();  //main thread is waiting on CountDownLatch to finish
                                Log.v(TAG, "Got response for the delete. Application is starting now !!!");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            Log.v(TAG, "Response received for delete lookup : " + deleteRequestResponseMap.get(selection).getRowsDeleted());
                            rows = deleteRequestResponseMap.get(selection).getRowsDeleted();
                        }
                    }
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } finally {
                    deleteRequestResponseMap.remove(selection);
                }

                if (canDelete) {
                    //  Delete a particular <key, value> pair represented by the selection
                    rows = db.delete(TABLE_NAME, COL_KEY_FIELD + "=?", new String[]{selection});
                }
            }
        } else {
            Log.v(TAG, "Selection is null");
        }

        Log.v(TAG, "****** Raman inside delete() END. Rows deleted : " + rows + " *****");
        return rows;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        Log.v(TAG, "***** Raman inside update() :-  Uri : " + uri + " , ContentValues : " + values.toString() + " , Selection : " + selection + " *****");

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        Log.v(TAG, "****** Raman inside getType(). Uri : " + uri + " *****");

        return null;
    }

    public void notifyChordRing(RamanMessage ramanMessage) {
        Log.v(TAG, "Sending message : " + ramanMessage.getMessage());
        new ClientTask(ramanMessage).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR);
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket socket = null;

            while (true) {
                try {
                    Log.v(TAG, "Server is listening on the node : " + MY_EMULATOR_NODE);

                    socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);

                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    final String msg = bufferedReader.readLine();
                    final String receivedMsg[] = msg.split(" : ");

                    final String originNode = receivedMsg[0];
                    final String senderNode = receivedMsg[1];
                    final String receiverNode = receivedMsg[2];
                    final String message = receivedMsg[3];

                    Log.v(TAG, "Received message : " + message + ", originNode : " + originNode + ", senderNode : " + senderNode + ", receiverNode : " + receiverNode);

                    try {

                        final String originHashNode = genHash("" + originNode);
                        final String senderHashNode = genHash("" + senderNode);
                        final String joinRequestToHashNode = genHash("" + Globals.JOIN_REQUEST_TO_NODE);

                        if (Globals.MSG_NODE_WAIT.equalsIgnoreCase(message)) {

                            while (null == myLookup && finalTime < Globals.TIMEOUT_VALUE) {
                                finalTime = System.currentTimeMillis() - initialTime;
                            }

                            if (null != nodeJoinCountDownLatch && nodeJoinCountDownLatch.getCount() == 1) {
                                nodeJoinCountDownLatch.countDown();
                            }

                            if (0 == singleNode.get()) {
                                final String prevNode = MY_EMULATOR_NODE;
                                final String nextNode = MY_EMULATOR_NODE;

                                Log.v(TAG, "Seems our server node is not up yet, " + MY_EMULATOR_NODE + " will do its work itself. Notifying the view about prevNode : " + prevNode + ", nextNode : " + nextNode);

                                myLookup = new Lookup(prevNode, nextNode);

                                singleNode.incrementAndGet();
                                publishProgress(new String[]{prevNode, nextNode});
                            }

                        }
                        if (Globals.MSG_NODE_JOIN.equalsIgnoreCase(message) && receiverNode.equalsIgnoreCase(Globals.JOIN_REQUEST_TO_NODE)) {
                            modifiedNodes.clear();

                            singleNode.incrementAndGet();
                            if (0 == chordRingMap.size() && senderNode.equalsIgnoreCase(Globals.JOIN_REQUEST_TO_NODE)) {
                                // If chord ring is not formed yet and sender node is NODE 5554
                                chordRingMap.put(new RamanKey(joinRequestToHashNode, Globals.JOIN_REQUEST_TO_NODE), new Lookup(Globals.JOIN_REQUEST_TO_NODE, Globals.JOIN_REQUEST_TO_NODE));
                                modifiedNodes.add(new RamanKey(joinRequestToHashNode, Globals.JOIN_REQUEST_TO_NODE));
                            } else if (0 == chordRingMap.size()) {
                                // If chord ring is not formed yet and sender node is NOT NODE 5554

                            } else if (1 == chordRingMap.size()) {
                                // If there is only 1 node in the ring which will be obviously Node 5554
                                chordRingMap.put(new RamanKey(joinRequestToHashNode, Globals.JOIN_REQUEST_TO_NODE), new Lookup(senderNode, senderNode));
                                modifiedNodes.add(new RamanKey(joinRequestToHashNode, Globals.JOIN_REQUEST_TO_NODE));

                                chordRingMap.put(new RamanKey(senderHashNode, senderNode), new Lookup(Globals.JOIN_REQUEST_TO_NODE, Globals.JOIN_REQUEST_TO_NODE));
                                modifiedNodes.add(new RamanKey(senderHashNode, senderNode));
                            } else {
                                Map<RamanKey, Lookup> sortedChordRingMap = sortByNodes(chordRingMap);

                                RamanKey currentKey = null, prevKey;
                                final RamanKey newKey = new RamanKey(senderHashNode, senderNode);

                                Lookup lookup;
                                String prevNode = null, hashPrevNode;
                                boolean updated = false;

                                for (Map.Entry<RamanKey, Lookup> entry : sortedChordRingMap.entrySet()) {
                                    Log.v(TAG, "Sorted:- Node : " + entry.getKey().getStrNode() + ", Hash Node : " + entry.getKey().getHashNode() + ", Next Node : " + entry.getValue().getNextNode() + ", Prev Node : " + entry.getValue().getPrevNode());

                                    currentKey = entry.getKey();
                                    lookup = entry.getValue();

                                    prevNode = chordRingMap.get(currentKey).getPrevNode();
                                    hashPrevNode = genHash(prevNode);
                                    prevKey = new RamanKey(hashPrevNode, prevNode);

                                    if (senderHashNode.compareTo(currentKey.getHashNode()) < 0) {
                                        // Insert new node in the chord ring with updated values
                                        chordRingMap.put(newKey, new Lookup(prevNode, currentKey.getStrNode()));
                                        modifiedNodes.add(newKey);

                                        // Update the values of previous node
                                        chordRingMap.replace(prevKey, new Lookup(chordRingMap.get(prevKey).getPrevNode(), senderNode));
                                        modifiedNodes.add(prevKey);

                                        // Update the values of current node
                                        chordRingMap.replace(currentKey, new Lookup(senderNode, lookup.getNextNode()));
                                        modifiedNodes.add(currentKey);


                                        updated = true;
                                        break;
                                    }
                                }

                                if (!updated) {
                                    // Means new node should join at the last position

                                    // Insert new node in the chord ring with updated values
                                    chordRingMap.put(newKey, new Lookup(currentKey.getStrNode(), chordRingMap.get(currentKey).getNextNode()));
                                    modifiedNodes.add(newKey);

                                    // Update the values of current node which was the last node previously
                                    chordRingMap.replace(currentKey, new Lookup(chordRingMap.get(currentKey).getPrevNode(), senderNode));
                                    modifiedNodes.add(currentKey);

                                    final String nextNode = chordRingMap.get(newKey).getNextNode();
                                    final String hashNextNode = genHash(chordRingMap.get(newKey).getNextNode());
                                    final RamanKey nextKey = new RamanKey(hashNextNode, nextNode);

                                    // Update the values of next node which is the first node
                                    chordRingMap.replace(nextKey, new Lookup(senderNode, chordRingMap.get(nextKey).getNextNode()));
                                    modifiedNodes.add(nextKey);
                                }
                            }

                            notifyChordRing(new RamanMessage(originNode, senderNode, receiverNode, Globals.MSG_CHORD_RING_UPDATE));

                        } else if (Globals.MSG_CHORD_RING_UPDATE.equalsIgnoreCase(message)) {
                            if (null != nodeJoinCountDownLatch && nodeJoinCountDownLatch.getCount() == 1) {
                                nodeJoinCountDownLatch.countDown();
                            }

                            final String prevNode = receivedMsg[4];
                            final String nextNode = receivedMsg[5];
                            myLookup = new Lookup(prevNode, nextNode);

                            if(!Globals.JOIN_REQUEST_TO_NODE.equalsIgnoreCase(senderNode)){
                                singleNode.set(5);
                            }
                            Log.v(TAG, "Received update of chord ring. Notifying the view about prevNode : " + prevNode + ", nextNode : " + nextNode);
                            publishProgress(new String[]{prevNode, nextNode});

                        } else if (Globals.MSG_INSERT_LOOKUP.equalsIgnoreCase(message)) {
                            ContentValues cv = new ContentValues();
                            final String key = receivedMsg[4];
                            final String value = receivedMsg[5];

                            insertRequestMap.put(key, new RamanMessage(originNode, senderNode, receiverNode, message));

                            cv.put(COL_KEY_FIELD, key);
                            cv.put(COL_VALUE_FIELD, value);
                            getContext().getContentResolver().insert(Globals.mUri, cv);

                        } else if (Globals.MSG_INSERT_LOOKUP_RESPONSE.equalsIgnoreCase(message)) {
                            Log.v(TAG, "Got Insert success response from " + senderNode + ". Notifying insertCountDownLatch to resume the main thread");
                            insertCountDownLatch.countDown();

                        } else if (Globals.MSG_QUERY_LOOKUP.equalsIgnoreCase(message)) {
                            final String key = receivedMsg[4];
                            queryRequestResponseMap.put(key, new RamanMessage(originNode, senderNode, receiverNode, message));

                            Cursor cursor = getContext().getContentResolver().query(Globals.mUri, null, key, null, null);

                            if (null != cursor) {
                                Log.v(TAG, "Query request sent by " + originNode + " executed successfully at " + MY_EMULATOR_NODE + ". So Sending back the response directly");

                                JSONObject jsonResult = buildJson(cursor);

                                RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, originNode, Globals.MSG_QUERY_LOOKUP_RESPONSE);
                                ramanMessage.setKey(key);
                                ramanMessage.setJsonString(jsonResult.toJSONString());

                                notifyChordRing(ramanMessage);
                            }

                        } else if (Globals.MSG_QUERY_LOOKUP_RESPONSE.equalsIgnoreCase(message)) {
                            final String key = receivedMsg[4];
                            final String jsonResponse = receivedMsg[5];

                            RamanMessage ramanMessage = new RamanMessage(originNode, senderNode, receiverNode, message);
                            ramanMessage.setKey(key);
                            ramanMessage.setJsonString(jsonResponse);

                            queryRequestResponseMap.put(key, ramanMessage);
                            Log.v(TAG, "Got Query success response : " + jsonResponse + " from " + senderNode + ". Notifying queryCountDownLatch to resume the main thread");
                            queryCountDownLatch.countDown();

                        } else if (Globals.MSG_DELETE_LOOKUP.equalsIgnoreCase(message)) {
                            final String key = receivedMsg[4];
                            deleteRequestResponseMap.put(key, new RamanMessage(originNode, senderNode, receiverNode, message));

                            int rows = getContext().getContentResolver().delete(Globals.mUri, key, null);

//                            if (rows > 0) {
                                Log.v(TAG, "Delete request sent by " + originNode + " executed successfully at " + MY_EMULATOR_NODE + ". So Sending back the response directly");

                                RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, originNode, Globals.MSG_DELETE_LOOKUP_RESPONSE);
                                ramanMessage.setKey(key);
                                ramanMessage.setRowsDeleted(rows);

                                notifyChordRing(ramanMessage);
//                            }

                        } else if (Globals.MSG_DELETE_LOOKUP_RESPONSE.equalsIgnoreCase(message)) {
                            final String key = receivedMsg[4];
                            final int rowsDeleted = Integer.parseInt(receivedMsg[5]);

                            if(rowsDeleted > -1 && null != deleteRequestResponseMap.get(key)) {
                                int prevDeletedRows = deleteRequestResponseMap.get(key).getRowsDeleted();

                                if(prevDeletedRows < rowsDeleted)
                                    prevDeletedRows = rowsDeleted;

                                RamanMessage ramanMessage = new RamanMessage(originNode, senderNode, receiverNode, message);
                                ramanMessage.setKey(key);
                                ramanMessage.setRowsDeleted(prevDeletedRows);

                                deleteRequestResponseMap.put(key, ramanMessage);
                            }

                            // This was to avoid issue in unordered delivery by my previous node
                            if (senderNode.equalsIgnoreCase(myLookup.getPrevNode())) {
                                response.incrementAndGet();
                            }

                            if (MY_EMULATOR_NODE.equalsIgnoreCase(senderNode)) {
                                response.incrementAndGet();
                            }

                            if (2 == response.get() || (null != deleteRequestResponseMap.get(key) && deleteRequestResponseMap.get(key).getRowsDeleted() > -1)) {
                                response.set(0);
                                Log.v(TAG, "Got Delete success response. Rows Deleted :  " + rowsDeleted + " from " + senderNode + ". Notifying deleteCountDownLatch to resume the main thread");
                                deleteCountDownLatch.countDown();
                            }
                        } else if (Globals.MSG_GLOBAL_DUMP_REQUEST.equalsIgnoreCase(message)) {
                            final RamanKey originRamanKey = new RamanKey(originHashNode, originNode);
                            final RamanMessage ramanSenderMessage = new RamanMessage(originNode, senderNode, receiverNode, message);

                            if (!MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                                // It means that if I originated this message than do not put in my response map, as I have already put it in when I originated this message
                                ArrayList<RamanMessage> list = new ArrayList<RamanMessage>();
                                list.add(ramanSenderMessage);
                                globalQueryResponseMap.put(originRamanKey, list);
                            }
                            // Hack to send origin node in sort order
                            Cursor cursor = getContext().getContentResolver().query(Globals.mUri, null, Globals.GLOBAL_QUERY, null, originNode);

                            if (null != cursor) {
                                Log.v(TAG, "Query request sent by " + originNode + " executed successfully at " + MY_EMULATOR_NODE + ". So Sending back my response for the global dump request");

                                JSONObject jsonResult = buildJson(cursor);
//                                if (jsonResult.size() > 0 || MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                                // It means send my response back to origin node only when I have something to show. Otherwise don't increase n/w traffic by sending empty message
                                // But.... If I give special authority to origin node :)
                                RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, originNode, Globals.MSG_GLOBAL_DUMP_RESPONSE);
                                ramanMessage.setJsonString(jsonResult.toJSONString());

                                notifyChordRing(ramanMessage);
//                                }
                            }
                        } else if (Globals.MSG_GLOBAL_DUMP_RESPONSE.equalsIgnoreCase(message)) {
                            // I will count down the latch only when I get the last response, i.e my own response
                            final String jsonResponse = receivedMsg[4];

                            Log.v(TAG, "Got Global dump response : " + jsonResponse + " from " + senderNode);

                            final RamanKey originRamanKey = new RamanKey(originHashNode, originNode);

                            RamanMessage ramanMessage = new RamanMessage(originNode, senderNode, receiverNode, message);
                            ramanMessage.setJsonString(jsonResponse);

                            ArrayList<RamanMessage> responseList = globalQueryResponseMap.get(originRamanKey);
                            responseList.add(ramanMessage);

                            globalQueryResponseMap.put(originRamanKey, responseList);

                            // This was to avoid issue in unordered delivery by my previous node
                            if (senderNode.equalsIgnoreCase(myLookup.getPrevNode())) {
                                response.incrementAndGet();
                            }

                            if (MY_EMULATOR_NODE.equalsIgnoreCase(senderNode)) {
                                response.incrementAndGet();
                            }

                            if (2 == response.get()) {
                                response.set(0);
                                Log.v(TAG, "Got Global dump response from all the nodes. Notifying queryCountDownLatch to resume the main thread");
                                globalCountDownLatch.countDown();
                            }
                        } else if (Globals.MSG_GLOBAL_DELETE_REQUEST.equalsIgnoreCase(message)) {
                            final RamanKey originRamanKey = new RamanKey(originHashNode, originNode);

                            if (!MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                                // It means that if I originated this message than do not put in my response map, as I have already put it in when I originated this message
                                globalDeleteResponseMap.put(originRamanKey, 0);
                            }
                            // Hack to send origin node in sort order
                            int rowsDeleted = getContext().getContentResolver().delete(Globals.mUri, Globals.GLOBAL_QUERY, new String[]{originNode});

                            Log.v(TAG, "Query request sent by " + originNode + " executed successfully at " + MY_EMULATOR_NODE + ". So Sending back my response for the global delete request");

//                            if (rowsDeleted > 0 || MY_EMULATOR_NODE.equalsIgnoreCase(originNode)) {
                            // It means send my response back to origin node only when I have something to show. Otherwise don't increase n/w traffic by sending empty message
                            // But....  I give special authority to origin node :)
                            RamanMessage ramanMessage = new RamanMessage(originNode, MY_EMULATOR_NODE, originNode, Globals.MSG_GLOBAL_DELETE_RESPONSE);
                            ramanMessage.setRowsDeleted(rowsDeleted);
                            notifyChordRing(ramanMessage);
//                            }
                        } else if (Globals.MSG_GLOBAL_DELETE_RESPONSE.equalsIgnoreCase(message)) {
                            // I will count down the latch only when I get the last response, i.e my own response
                            final int rowsDeleted = Integer.parseInt(receivedMsg[4]);

                            Log.v(TAG, "Got Global delete response from " + senderNode + ", Rows deleted : " + rowsDeleted);

                            final RamanKey originRamanKey = new RamanKey(originHashNode, originNode);

                            int prevDeletedRows = globalDeleteResponseMap.get(originRamanKey);
                            globalDeleteResponseMap.put(originRamanKey, (prevDeletedRows + rowsDeleted));

                            // This was to avoid issue in unordered delivery by my previous node
                            if (senderNode.equalsIgnoreCase(myLookup.getPrevNode())) {
                                response.incrementAndGet();
                            }

                            if (MY_EMULATOR_NODE.equalsIgnoreCase(senderNode)) {
                                response.incrementAndGet();
                            }

                            if (2 == response.get()) {
                                response.set(0);
                                Log.v(TAG, "Got Global delete response from all the nodes. Notifying qlobalCountDownLatch to resume the main thread");
                                globalCountDownLatch.countDown();
                            }
                        }
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                } catch (SocketTimeoutException e) {
                    Log.v(TAG, "ServerTask SocketTimeoutException");

                } catch (IOException e) {
                    Log.v(TAG, "Error in Server Task IO Exception");

                } finally {
                    if (null != socket && !socket.isClosed()) {
                        Log.v(TAG, "Closing the socket accepted by the Server");
                        try {
                            socket.close();
                        } catch (IOException e) {
                            Log.v(TAG, "Error in Server Task IO Exception when closing socket");
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        protected void onProgressUpdate(String... receivedMsg) {
            final String prevNode = receivedMsg[0];
            final String nextNode = receivedMsg[1];

            sendMessage(Globals.NEXT_PREV_NODE_LISTENER, prevNode, nextNode);
        }
    }

    private JSONObject buildJson(Cursor cursor) {
        JSONObject jsonObject = new JSONObject();

        try {
            if (cursor == null) {
                Log.v(TAG, "Passed cursor is null");
                throw new Exception();
            }

            if (cursor.moveToFirst()) {
                do {
                    int keyIndex = cursor.getColumnIndex(COL_KEY_FIELD);
                    int valueIndex = cursor.getColumnIndex(COL_VALUE_FIELD);

                    if (keyIndex == -1 || valueIndex == -1) {
                        Log.v(TAG, "Wrong columns");
                        throw new Exception();
                    } else {
                        String strKey = cursor.getString(keyIndex);
                        String strValue = cursor.getString(valueIndex);

                        jsonObject.put(strKey, strValue);
                    }
                } while (cursor.moveToNext());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != cursor && !cursor.isClosed()) {
                cursor.close();
            }
            return jsonObject;
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        private RamanMessage ramanMessage;

        ClientTask(RamanMessage ramanMessage) {
            this.ramanMessage = ramanMessage;
        }

        @Override
        protected Void doInBackground(String... msgs) {
            Log.v(TAG, "Entered ClientTask doInBackground()");
            try {
                if (MY_EMULATOR_NODE.equalsIgnoreCase(Globals.JOIN_REQUEST_TO_NODE) && Globals.MSG_CHORD_RING_UPDATE.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket[] sockets = new Socket[modifiedNodes.size()];
                    for (int i = 0; i < modifiedNodes.size(); i++) {
                        sockets[i] = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                (Integer.parseInt(modifiedNodes.get(i).getStrNode()) * 2));
                        sockets[i].setTcpNoDelay(true);

                        OutputStream outputStream = sockets[i].getOutputStream();
                        PrintWriter printWriter = new PrintWriter(outputStream, true);
                        printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + modifiedNodes.get(i).getStrNode() + " : " + Globals.MSG_CHORD_RING_UPDATE + " : " + chordRingMap.get(modifiedNodes.get(i)).getPrevNode() + " : " + chordRingMap.get(modifiedNodes.get(i)).getNextNode());

                        Log.v(TAG, "Sent Chord Ring Update Msg :-  Sender Node : " + MY_EMULATOR_NODE + ", Receiver Node : " + modifiedNodes.get(i).getStrNode() + ", Message : " + Globals.MSG_CHORD_RING_UPDATE + ", Prev Node : " + chordRingMap.get(modifiedNodes.get(i)).getPrevNode() + ", Next Node : " + chordRingMap.get(modifiedNodes.get(i)).getNextNode());

                        printWriter.close();
                        sockets[i].close();
                    }
                } else if (Globals.MSG_NODE_WAIT.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_NODE_WAIT);
                    Log.v(TAG, "Sent Node Join Wait :- Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_NODE_WAIT);

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_NODE_JOIN.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_NODE_JOIN);
                    Log.v(TAG, "Sent Node Join Request :- Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_NODE_JOIN);

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_INSERT_LOOKUP.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_INSERT_LOOKUP + " : " + ramanMessage.getKey() + " : " + ramanMessage.getValue());
                    Log.v(TAG, "Sent Insert Lookup Request :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_INSERT_LOOKUP + ", Key : " + ramanMessage.getKey() + ", Value : " + ramanMessage.getValue());

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_INSERT_LOOKUP_RESPONSE.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_INSERT_LOOKUP_RESPONSE);
                    Log.v(TAG, "Sent Insert Success Response :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_INSERT_LOOKUP_RESPONSE);

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_QUERY_LOOKUP.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_QUERY_LOOKUP + " : " + ramanMessage.getKey());
                    Log.v(TAG, "Sent Query Lookup Request :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + Integer.parseInt(myLookup.getNextNode()) + ",  Message : " + Globals.MSG_QUERY_LOOKUP + ", Key : " + ramanMessage.getKey());

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_QUERY_LOOKUP_RESPONSE.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_QUERY_LOOKUP_RESPONSE + " : " + ramanMessage.getKey() + " : " + ramanMessage.getJsonString());
                    Log.v(TAG, "Sent Query Lookup Response :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_QUERY_LOOKUP_RESPONSE + ", Key : " + ramanMessage.getKey() + ", JSON Response : " + ramanMessage.getJsonString());

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_DELETE_LOOKUP.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_DELETE_LOOKUP + " : " + ramanMessage.getKey());
                    Log.v(TAG, "Sent Delete Lookup Request :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + Integer.parseInt(myLookup.getNextNode()) + ",  Message : " + Globals.MSG_DELETE_LOOKUP + ", Key : " + ramanMessage.getKey());

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_DELETE_LOOKUP_RESPONSE.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_DELETE_LOOKUP_RESPONSE + " : " + ramanMessage.getKey() + " : " + ramanMessage.getRowsDeleted());
                    Log.v(TAG, "Sent Delete Lookup Response :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_DELETE_LOOKUP_RESPONSE + ", Key : " + ramanMessage.getKey() + ", Rows Deleted : " + ramanMessage.getRowsDeleted());

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_GLOBAL_DUMP_REQUEST.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_GLOBAL_DUMP_REQUEST);
                    Log.v(TAG, "Sent Global Dump Request :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + Integer.parseInt(myLookup.getNextNode()) + ",  Message : " + Globals.MSG_GLOBAL_DUMP_REQUEST);

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_GLOBAL_DUMP_RESPONSE.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_GLOBAL_DUMP_RESPONSE + " : " + ramanMessage.getJsonString());
                    Log.v(TAG, "Sent Global Dump Response :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_GLOBAL_DUMP_RESPONSE + ", JSON Response : " + ramanMessage.getJsonString());

                    printWriter.close();
                    socket.close();
                } else if (Globals.MSG_GLOBAL_DELETE_REQUEST.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_GLOBAL_DELETE_REQUEST);
                    Log.v(TAG, "Sent Global Delete Request :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + Integer.parseInt(myLookup.getNextNode()) + ",  Message : " + Globals.MSG_GLOBAL_DELETE_REQUEST);

                    printWriter.close();
                    socket.close();

                } else if (Globals.MSG_GLOBAL_DELETE_RESPONSE.equalsIgnoreCase(ramanMessage.getMessage())) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), (Integer.parseInt(ramanMessage.getReceiverNode()) * 2));
                    socket.setTcpNoDelay(true);
                    socket.setSoTimeout(Globals.TIMEOUT_VALUE);

                    OutputStream outputStream = socket.getOutputStream();
                    PrintWriter printWriter = new PrintWriter(outputStream, true);

                    printWriter.println(ramanMessage.getOriginNode() + " : " + ramanMessage.getSenderNode() + " : " + ramanMessage.getReceiverNode() + " : " + Globals.MSG_GLOBAL_DELETE_RESPONSE + " : " + ramanMessage.getRowsDeleted());
                    Log.v(TAG, "Sent Global Delete Response :-  Origin Node : " + ramanMessage.getOriginNode() + ", Sender Node : " + ramanMessage.getSenderNode() + ", Receiver Node : " + ramanMessage.getReceiverNode() + ",  Message : " + Globals.MSG_GLOBAL_DELETE_RESPONSE + ", Rows Deleted : " + ramanMessage.getRowsDeleted());

                    printWriter.close();
                    socket.close();
                }
            } catch (UnknownHostException e) {
                Log.v(TAG, "ClientTask UnknownHostException");
            } catch (SocketTimeoutException e) {
                Log.v(TAG, "ClientTask UnknownHostException");
            } catch (EOFException e) {
                Log.v(TAG, "ClientTask EOFException");
            } catch (IOException e) {
                Log.v(TAG, "ClientTask IOException");
            } finally {
//                if (Globals.MSG_NODE_JOIN.equalsIgnoreCase(ramanMessage.getMessage())) {
//                    Message timerMessage = new Message();
//                    Bundle bundle = new Bundle();
//                    timerMessage.setData(bundle);
//                    myTimerHandler.sendMessageDelayed(timerMessage, Globals.TIMEOUT_VALUE);
//                }
            }
            return null;
        }

    }

    private final Handler myTimerHandler = new Handler() {
        @Override
        public void handleMessage(Message msg) {
        }

        private void sendMessage(String action, String prevNode, String nextNode) {
            Intent intent = new Intent(action);
            intent.putExtra(Globals.TXT_PREV_NODE, prevNode);
            intent.putExtra(Globals.TXT_NEXT_NODE, nextNode);
            LocalBroadcastManager.getInstance(getContext()).sendBroadcast(intent);
        }
    };

    private static Map<RamanKey, Lookup> sortByNodes(Map<RamanKey, Lookup> unSortedMap) {
        List<Map.Entry<RamanKey, Lookup>> list = new LinkedList<Map.Entry<RamanKey, Lookup>>(unSortedMap.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<RamanKey, Lookup>>() {
            public int compare(Map.Entry<RamanKey, Lookup> o1, Map.Entry<RamanKey, Lookup> o2) {
                if (o1.getKey().getHashNode().compareTo(o2.getKey().getHashNode()) < 0) {
                    return -1;
                } else if (o1.getKey().getHashNode().compareTo(o2.getKey().getHashNode()) > 0) {
                    return 1;
                } else {
                    return 0;
                }
            }
        });

        Map<RamanKey, Lookup> sortedMap = new LinkedHashMap<RamanKey, Lookup>();
        for (Iterator<Map.Entry<RamanKey, Lookup>> it = list.iterator(); it.hasNext(); ) {
            Map.Entry<RamanKey, Lookup> entry = it.next();
            sortedMap.put(entry.getKey(), entry.getValue());
        }
        return sortedMap;
    }

    public void sendMessage(String action, String prevNode, String nextNode) {
        Intent intent = new Intent(action);
        intent.putExtra(Globals.TXT_PREV_NODE, prevNode);
        intent.putExtra(Globals.TXT_NEXT_NODE, nextNode);
        LocalBroadcastManager.getInstance(getContext()).sendBroadcast(intent);
    }


    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}