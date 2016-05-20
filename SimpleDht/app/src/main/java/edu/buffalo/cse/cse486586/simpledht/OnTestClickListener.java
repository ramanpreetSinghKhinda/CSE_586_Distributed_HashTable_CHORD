package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.EditText;
import android.widget.TextView;

public class OnTestClickListener implements OnClickListener {

    private static final String TAG = OnTestClickListener.class.getName();
    private static final int TEST_CNT = 1;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    private final TextView mTextView;
    private final EditText mEditText;
    private final ContentResolver mContentResolver;
    private final Uri mUri;
    private final ContentValues[] mContentValues;
    private final String myNode;

    public OnTestClickListener(TextView _tv, ContentResolver _cr, String _mn, EditText _et) {
        mTextView = _tv;
        mEditText = _et;
        mContentResolver = _cr;
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        myNode = _mn;
        mContentValues = initTestValues();
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private ContentValues[] initTestValues() {
        ContentValues[] cv = new ContentValues[TEST_CNT];

        for (int i = 0; i < TEST_CNT; i++) {
            cv[i] = new ContentValues();
            cv[i].put(KEY_FIELD, "key : " + myNode + " : " + Integer.toString(i));
            cv[i].put(VALUE_FIELD, "val : " + myNode + " : " + Integer.toString(i));
        }

        return cv;
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.btn_test_insert:
                // Test Insert
                if (testInsert()) {
                    Log.v(TAG, "Insert Success");
                    mTextView.append("\nInsert success\n");
                } else {
                    Log.v(TAG, "Insert Fail");
                    mTextView.append("\nInsert fail\n");
                }

//                new TestInsertTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
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

//                new TestQueryTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR);
                break;

        }
    }

    private boolean testInsert() {
        try {
            String key = mEditText.getText().toString();
            ContentValues cv = new ContentValues();
            cv.put(KEY_FIELD, key);
            cv.put(VALUE_FIELD, key);

            mContentResolver.insert(mUri, cv);

//                for (int i = 0; i < TEST_CNT; i++) {
//                    mContentResolver.insert(mUri, mContentValues[i]);
//                }
        } catch (Exception e) {
            Log.e(TAG, e.toString());
            return false;
        }

        return true;
    }


    private class TestInsertTask extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            if (testInsert()) {
                Log.v(TAG, "Insert Success");
                publishProgress("\nInsert success\n");
            } else {
                Log.v(TAG, "Insert Fail");
                publishProgress("\nInsert fail\n");
                return null;
            }
            return null;
        }

        protected void onProgressUpdate(String... strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testInsert() {
            try {
                String key = mEditText.getText().toString();
                ContentValues cv = new ContentValues();
                cv.put(KEY_FIELD, key);
                cv.put(VALUE_FIELD, key);

                mContentResolver.insert(mUri, cv);

//                for (int i = 0; i < TEST_CNT; i++) {
//                    mContentResolver.insert(mUri, mContentValues[i]);
//                }
            } catch (Exception e) {
                Log.e(TAG, e.toString());
                return false;
            }

            return true;
        }
    }

    private boolean testQuery() {
        try {
            for (int i = 0; i < TEST_CNT; i++) {
                String key = mEditText.getText().toString();

//                    String key = (String) mContentValues[i].get(KEY_FIELD);
//                    String val = (String) mContentValues[i].get(VALUE_FIELD);

                Cursor resultCursor = mContentResolver.query(mUri, null,
                        key, null, null);
                if (resultCursor == null) {
                    Log.e(TAG, "Result null");
                    throw new Exception();
                }

                int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
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
            }
        } catch (Exception e) {
            return false;
        }

        return true;
    }

    private class TestQueryTask extends AsyncTask<Void, String, Void> {

        @Override
        protected Void doInBackground(Void... params) {
            if (testQuery()) {
                Log.v(TAG, "Query Success");
                publishProgress("\nQuery success\n");
            } else {
                Log.v(TAG, "Query fail");
                publishProgress("\nQuery fail\n");
            }

            return null;
        }

        protected void onProgressUpdate(String... strings) {
            mTextView.append(strings[0]);

            return;
        }

        private boolean testQuery() {
            try {
                for (int i = 0; i < TEST_CNT; i++) {
                    String key = mEditText.getText().toString();

//                    String key = (String) mContentValues[i].get(KEY_FIELD);
//                    String val = (String) mContentValues[i].get(VALUE_FIELD);

                    Cursor resultCursor = mContentResolver.query(mUri, null,
                            key, null, null);
                    if (resultCursor == null) {
                        Log.e(TAG, "Result null");
                        throw new Exception();
                    }

                    int keyIndex = resultCursor.getColumnIndex(KEY_FIELD);
                    int valueIndex = resultCursor.getColumnIndex(VALUE_FIELD);
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
                }
            } catch (Exception e) {
                return false;
            }

            return true;
        }
    }

}
