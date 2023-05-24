package hamburg.dbis.recovery;

import hamburg.dbis.persistence.PersistenceManager;

import java.io.*;
import java.util.ArrayList;
import java.util.Hashtable;

public class RecoveryManager {

    static final private RecoveryManager _manager;
    private Hashtable<Integer, Integer> data;

    // TODO Add class variables if necessary
    static {
        try {
            _manager = new RecoveryManager();
        } catch (Throwable e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private RecoveryManager() {
        // TODO Initialize class variables if necessary
        data = new Hashtable<>();
    }

    static public RecoveryManager getInstance() {
        return _manager;
    }

    public void startRecoveryPrePhase() {

        // Iterate over the log entries and apply the necessary recovery operations
        try (BufferedReader reader = new BufferedReader(new FileReader("log.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                String operation = entry[2];
                int transactionID = Integer.parseInt(entry[1]);
                String LSN = entry[0];
                if (operation.equals("EOT")) {
                    data.put(transactionID, Integer.parseInt(LSN));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startRecovery() {
        startRecoveryPrePhase();
        startRecoveryPhaseTwo();
        data = new Hashtable<>();
    }

    public void startRecoveryPhaseTwo() {
        // TODO Perform recovery
        int lastTransactionID = retrieveLastUsedTransactionID();
        int lastUsedLSN = retrieveLastUsedLSN();
        int lastUsedLSNBeforeRecovery = lastUsedLSN;

        // Iterate over the log entries and apply the necessary recovery operations
        try (BufferedReader reader = new BufferedReader(new FileReader("log.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                int transactionID = Integer.parseInt(entry[1]);
                String operation = entry[2];
                String LSN = entry[0];

                if (transactionID <= lastTransactionID && Integer.parseInt(LSN) <= lastUsedLSNBeforeRecovery
                        && data.get(transactionID) != null && data.get(transactionID) >= Integer.parseInt(LSN)) {
                    lastUsedLSN++;
                    // Apply recovery operation based on the operation type
                    if (operation.equals("EOT")) {
                        // Handle end-of-transaction
                        // Perform necessary recovery actions
                        writeLogEntry(generateLogEntry(lastUsedLSN, transactionID, "EOT"));
                        performEndOfTransactionRecovery(transactionID);
                    } else {
                        // Handle data modification operation
                        int pageID = Integer.parseInt(entry[2]);
                        String data = lastUsedLSN + "," + entry[3];
                        writeLogEntry(generateLogEntry(lastUsedLSN, transactionID, pageID, data));
                        // Perform necessary recovery actions for data modification
                        performDataModificationRecovery(transactionID, pageID, data);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String generateLogEntry(int lastUsedLSN, int transactionID, int pageID, String data) {
        return lastUsedLSN + "," + transactionID + "," + pageID + "," + data;
    }

    private String generateLogEntry(int lastUsedLSN, int transactionID, String operation) {
        return lastUsedLSN + "," + transactionID + "," + operation;
    }

    public int retrieveLastUsedTransactionID() {
        // TODO Implement the logic to retrieve the last used transaction ID from the
        // log (if present)
        // For example, you could read the log file and extract the last transaction ID
        // from the entries
        int lastTransactionID = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader("log.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] entry = line.split(",");
                int transactionID = Integer.parseInt(entry[0]);

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

    // Perform necessary recovery actions for end-of-transaction
    private void performEndOfTransactionRecovery(int transactionID) {
        // TODO Perform necessary recovery actions for end-of-transaction
    }

    // Perform necessary recovery actions for data modification
    private void performDataModificationRecovery(int transactionID, int pageID, String data) {
        // TODO Perform necessary recovery actions for data modification
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
            FileWriter fileWriter = new FileWriter("log.txt", true);
            fileWriter.write(entry + System.lineSeparator());
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int retrieveLastUsedLSN() {
        int lastUsedLSN = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader("log.txt"))) {
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
