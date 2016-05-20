package edu.buffalo.cse.cse486586.simpledht;

import android.app.Activity;
import android.content.BroadcastReceiver;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.res.Resources;
import android.database.Cursor;
import android.os.Bundle;
import android.support.v4.content.LocalBroadcastManager;
import android.telephony.TelephonyManager;
import android.text.Html;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;


class RamanKey {
    private String hashNode;
    private String strNode;

    RamanKey(String hashNode, String strNode) {
        this.hashNode = hashNode;
        this.strNode = strNode;
    }

    public void setHashNode(String hashNode) {
        this.hashNode = hashNode;
    }

    public void setStrNode(String strNode) {
        this.strNode = strNode;
    }

    public String getHashNode() {
        return hashNode;
    }

    public String getStrNode() {
        return strNode;
    }

    public int hashCode() {
        return hashNode.hashCode();
    }

    public boolean equals(Object obj) {
        if (obj instanceof RamanKey) {
            RamanKey ramanKey = (RamanKey) obj;
            return (ramanKey.getHashNode().equals(this.getHashNode()));
        } else {
            return false;
        }
    }
}

class RamanMessage {
    private String originNode;
    private String senderNode;
    private String receiverNode;
    private String message;
    private String key;
    private String value;
    private String jsonString;
    private int rowsDeleted;

    RamanMessage(String originNode, String senderNode, String receiverNode, String message) {
        this.originNode = originNode;
        this.senderNode = senderNode;
        this.receiverNode = receiverNode;
        this.message = message;
    }

    public void setOriginNode(String originNode) {
        this.originNode = originNode;
    }

    public void setSenderNode(String senderNode) {
        this.senderNode = senderNode;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public void setReceiverNode(String receiverNode) {
        this.receiverNode = receiverNode;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void setJsonString(String jsonString) {
        this.jsonString = jsonString;
    }

    public void setRowsDeleted(int rowsDeleted) {
        this.rowsDeleted = rowsDeleted;
    }

    public String getOriginNode() {
        return originNode;
    }

    public String getSenderNode() {
        return senderNode;
    }

    public String getReceiverNode() {
        return receiverNode;
    }

    public String getMessage() {
        return message;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public String getJsonString() {
        return jsonString;
    }

    public int getRowsDeleted() {
        return rowsDeleted;
    }
}

class Lookup {
    private String nextNode;
    private String prevNode;

    Lookup(String prevNode, String nextNode) {
        this.prevNode = prevNode;
        this.nextNode = nextNode;
    }

    public void setPrevNode(String prevNode) {
        this.prevNode = prevNode;
    }

    public void setNextNode(String nextNode) {
        this.nextNode = nextNode;
    }

    public String getPrevNode() {
        return prevNode;
    }

    public String getNextNode() {
        return nextNode;
    }

}

public class SimpleDhtActivity extends Activity implements View.OnClickListener {
    private static final String TAG = SimpleDhtActivity.class.getName();

    private Resources res;

    private Button btnDump, btnDelete, btnTestInsert, btnTestQuery;
    private TextView mTextView, txtPrevNode, txtNextNode;
    private EditText mEditText;

    private String MY_PORT, MY_EMULATOR_NODE;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        res = getResources();

        mTextView = (TextView) findViewById(R.id.textView1);
        mTextView.setMovementMethod(new ScrollingMovementMethod());

        txtPrevNode = (TextView) findViewById(R.id.txt_prev_node);
        txtNextNode = (TextView) findViewById(R.id.txt_next_node);

        mEditText = (EditText) findViewById(R.id.edit_txt);

        btnDump = (Button) findViewById(R.id.button1);
        btnDelete = (Button) findViewById(R.id.button2);
        btnTestInsert = (Button) findViewById(R.id.btn_test_insert);
        btnTestQuery = (Button) findViewById(R.id.btn_test_query);

        /*
         * Calculate the port number that this AVD listens on.
         * It is just a hack that I came up with to get around the networking limitations of AVDs.
         * The explanation is provided in the PA1 spec.
         */
        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);

        MY_EMULATOR_NODE = String.valueOf((Integer.parseInt(portStr)));
        MY_PORT = String.valueOf((Integer.parseInt(portStr) * 2));

        LocalBroadcastManager.getInstance(this).registerReceiver(nextPrevNodeListener, new IntentFilter(Globals.NEXT_PREV_NODE_LISTENER));
    }

    @Override
    protected void onResume() {
        super.onResume();
        btnDump.setOnClickListener(this);
        btnDelete.setOnClickListener(this);
        btnTestInsert.setOnClickListener(this);
        btnTestQuery.setOnClickListener(this);
    }

    // Will be called whenever the next prev node gets change
    private BroadcastReceiver nextPrevNodeListener = new BroadcastReceiver() {
        @Override
        public void onReceive(Context context, Intent intent) {
            String strPrevNode = intent.getStringExtra(Globals.TXT_PREV_NODE);
            String strNextNode = intent.getStringExtra(Globals.TXT_NEXT_NODE);

            txtPrevNode.setText(strPrevNode);
            txtNextNode.setText(strNextNode);
        }
    };

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.button1:
                if (mEditText.getText().toString().equalsIgnoreCase(Globals.LOCAL_QUERY)) {
                    // Local Dump
                    if (showDump(Globals.LOCAL_QUERY)) {
                        Log.v(TAG, "Local Dump Success");
                        mTextView.append("\nLocal Dump Success\n");
                    } else {
                        Log.v(TAG, "Local Dump Fail");
                        mTextView.append("\nLocal Dump Fail\n");
                    }
                } else if (mEditText.getText().toString().equalsIgnoreCase(Globals.GLOBAL_QUERY)) {
                    // Global Dump
                    if (showDump(Globals.GLOBAL_QUERY)) {
                        Log.v(TAG, "Global Dump Success");
                        mTextView.append("\nGlobal Dump Success\n");
                    } else {
                        Log.v(TAG, "Global Dump Fail");
                        mTextView.append("\nGlobal Dump Fail\n");
                    }
                } else {
                    mTextView.append("\nEntered Key is not correct. It can handle either @ or * queries\n");
                }

                break;

            case R.id.button2:
                deleteData(mEditText.getText().toString());
                break;

            case R.id.btn_test_insert:
                // Test Insert
                if (testInsert()) {
                    Log.v(TAG, "Insert Success");
                    mTextView.append("\nInsert Success\n");
                } else {
                    Log.v(TAG, "Insert Fail");
                    mTextView.append("\nInsert Fail\n");
                }
                break;

            case R.id.btn_test_query:
                // Test Query
                if (testQuery()) {
                    Log.v(TAG, "Query Success");
                    mTextView.append("\nQuery success\n");
                } else {
                    Log.v(TAG, "Query fail");
                    mTextView.append("\nQuery fail\n");
                }
                break;
        }
    }

    private boolean showDump(String query) {
        Cursor resultCursor = null;
        boolean success = true;

        try {
            resultCursor = getContentResolver().query(Globals.mUri, null, query, null, null);

            if (resultCursor == null) {
                success = false;

                Log.v(TAG, "Result null");
                throw new Exception();
            }

            if (resultCursor.moveToFirst()) {
                do {
                    int keyIndex = resultCursor.getColumnIndex(Globals.KEY_FIELD);
                    int valueIndex = resultCursor.getColumnIndex(Globals.VALUE_FIELD);

                    if (keyIndex == -1 || valueIndex == -1) {
                        success = false;

                        Log.v(TAG, "Wrong columns");
                        resultCursor.close();
                        throw new Exception();
                    } else {
                        String strKey = resultCursor.getString(keyIndex);
                        String strValue = resultCursor.getString(valueIndex);

                        String displayedMsg = "\nKey : " + strKey + "\nValue : " + strValue;

                        // Displaying Color text so as to differentiate messages sent by different devices
                        String colorStrReceived = "<font color='" + getColor(MY_PORT) + "'>" + displayedMsg + "</font>";
                        mTextView.append("\n ");
                        mTextView.append(Html.fromHtml(colorStrReceived));
                    }

                } while (resultCursor.moveToNext());
            }
        } catch (Exception e) {
            success = false;

            Log.v(TAG, "Exception in showDump()");
            e.printStackTrace();
        } finally {
            if (null != resultCursor && !resultCursor.isClosed()) {
                resultCursor.close();
            }
        }

        return success;
    }

    private void deleteData(String query) {
        int rowsDeleted = 0;
        try {
            rowsDeleted = getContentResolver().delete(Globals.mUri, query, null);
            String displayedMsg = "\nDeleted : " + rowsDeleted + " rows";

            // Displaying Color text so as to differentiate messages sent by different devices
            String colorStrReceived = "<font color='" + getColor(MY_PORT) + "'>" + displayedMsg + "</font>";
            mTextView.append("\n ");
            mTextView.append(Html.fromHtml(colorStrReceived));
        } catch (Exception e) {
            mTextView.append("\nDelete Query Failed\n");
        }
    }

    private boolean testInsert() {
        try {
            String key = mEditText.getText().toString();
            ContentValues cv = new ContentValues();
            cv.put(Globals.KEY_FIELD, key);
            cv.put(Globals.VALUE_FIELD, key);

            getContentResolver().insert(Globals.mUri, cv);
        } catch (Exception e) {
            Log.e(TAG, e.toString());
            return false;
        }

        return true;
    }

    private boolean testQuery() {
        try {
            String key = mEditText.getText().toString();
            Cursor resultCursor = getContentResolver().query(Globals.mUri, null,
                    key, null, null);
            if (resultCursor == null) {
                Log.e(TAG, "Result null");
                throw new Exception();
            }

            int keyIndex = resultCursor.getColumnIndex(Globals.KEY_FIELD);
            int valueIndex = resultCursor.getColumnIndex(Globals.VALUE_FIELD);
            if (keyIndex == -1 || valueIndex == -1) {
                Log.e(TAG, "Wrong columns");
                resultCursor.close();
                throw new Exception();
            }

            resultCursor.moveToFirst();

            if (!(resultCursor.isFirst() && resultCursor.isLast())) {
                Log.e(TAG, "Wrong number of rows");
                resultCursor.close();
                throw new Exception();
            }

            String returnKey = resultCursor.getString(keyIndex);
            String returnValue = resultCursor.getString(valueIndex);
            if (!(returnKey.equals(key) && returnValue.equals(key))) {
                Log.e(TAG, "(key, value) pairs don't match\n");
                resultCursor.close();
                throw new Exception();
            }

            resultCursor.close();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private int getColor(String port) {
        int textColor = res.getColor(R.color.my_port);

        if (Globals.REMOTE_PORT0.contains(port)) {
            textColor = res.getColor(R.color.remote_port0);
        } else if (Globals.REMOTE_PORT1.contains(port)) {
            textColor = res.getColor(R.color.remote_port1);
        } else if (Globals.REMOTE_PORT2.contains(port)) {
            textColor = res.getColor(R.color.remote_port2);
        } else if (Globals.REMOTE_PORT3.contains(port)) {
            textColor = res.getColor(R.color.remote_port3);
        } else if (Globals.REMOTE_PORT4.contains(port)) {
            textColor = res.getColor(R.color.remote_port4);
        }

        return textColor;
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
