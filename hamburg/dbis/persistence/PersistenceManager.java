package hamburg.dbis.persistence;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;

public class PersistenceManager {

    static final private PersistenceManager _manager;

    // TODO Add class variables if necessary
    private int lastUsedTransactionID;
    private int lastUsedLSN;
    private Hashtable<Integer, ArrayList<String>> buffer; // Buffer to store dataset for ongoing transactions
    private String logFilePath; // File path for storing log entries

    static {
        try {
            _manager = new PersistenceManager();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private PersistenceManager() {
        // TODO Get the last used transaction id from the log (if present) at startup
        // TODO Initialize class variables if necessary
        logFilePath = "log.txt";
        lastUsedTransactionID = retrieveLastUsedTransactionID();
        lastUsedLSN = retrieveLastUsedLSN();
        buffer = new Hashtable<>();
    }

    static public PersistenceManager getInstance() {
        return _manager;
    }

    public synchronized int beginTransaction() {
        // TODO return a valid transaction id to the client
        lastUsedTransactionID++;
        ArrayList<String> data = new ArrayList<>();
        buffer.put(lastUsedTransactionID, data); // Initialize an empty dataset for the new transaction
        return lastUsedTransactionID;
    }

    public void commit(int taid) {
        // TODO handle commits
        System.out.println("Commit");
        ArrayList<String> dataset = buffer.get(taid); // Retrieve the dataset for the completed transaction
        writeLogEntry(generateLogEntry(taid, "EOT"));
        
        for(String data: dataset)
        {
            String[] value = data.split(",");
            int pageID = Integer.parseInt(value[2]);
            String userData = generatePageEntry(value[0],value[3]);
            writePage(pageID, userData); // Write the dataset to the corresponding page
        }
        buffer.remove(taid); // Remove the transaction from the buffer

        // Check if buffer size exceeds the threshold for writing committed datasets
        if (buffer.size() > 5) {
            writeCommittedDatasets();
        }
    }

    public void write(int taid, int pageid, String data) {
        // TODO handle writes of Transaction taid on page pageid with data
        String entry = generateLogEntry(taid, pageid, data);
        writeLogEntry(entry);
        buffer.get(taid).add(entry); // Update the dataset in the buffer

        // Check if buffer size exceeds the threshold for writing committed datasets
        if (buffer.size() > 5) {
            writeCommittedDatasets();
        }
    }

    private void writePage(int pageID, String data) {
        try {
            FileWriter fileWriter = new FileWriter(pageID + ".txt");
            fileWriter.write(data);
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeLogEntry(String entry) {
        try {
            FileWriter fileWriter = new FileWriter(logFilePath, true);
            fileWriter.write(entry + System.lineSeparator());
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeCommittedDatasets() {
        // Iterate over the buffer and write datasets related to committed transactions to the persistent storage
        for (int taid : buffer.keySet()) {
            if (isTransactionCommitted(taid)) {
                ArrayList<String> dataset = buffer.get(taid);
                for(String data: dataset)
                {
                    writePage(taid, data); // Write the dataset to the corresponding page
                }
                buffer.remove(taid);
            }
        }
    }

    private boolean isTransactionCommitted(int taid) {
        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                int transactionID = Integer.parseInt(entry[0]);
                String operation = entry[1];
    
                if (transactionID == taid && operation.equals("EOT")) {
                    return true; // Transaction is committed
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    
        return false; // Transaction is not committed
    }

    private String generateLogEntry(int transactionID, int pageID, String data) {
        lastUsedLSN += 1;
        return lastUsedLSN + "," + transactionID + "," + pageID + "," + data;
    }

    private String generateLogEntry(int transactionID, String operation) {
        lastUsedLSN += 1;
        return lastUsedLSN + "," +transactionID + "," + operation;
    }

    private String generatePageEntry(String lsn, String content){
        return lsn + "," + content;
    }

    private int retrieveLastUsedTransactionID() {
        // TODO Implement the logic to retrieve the last used transaction ID from the log (if present)
        // For example, you could read the log file and extract the last transaction ID from the entries
        int lastTransactionID = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                int transactionID = Integer.parseInt(entry[1]);

                // Update the lastTransactionID if the current transactionID is greater
                if (transactionID > lastTransactionID) {
                    lastTransactionID = transactionID;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lastTransactionID;
    }

    private int retrieveLastUsedLSN(){
        int lastUsedLSN = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader(logFilePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                int transactionID = Integer.parseInt(entry[0]);
                // Update the lastTransactionID if the current transactionID is greater
                if (transactionID > lastUsedLSN) {
                    lastUsedLSN = transactionID;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return lastUsedLSN;
    }
}
