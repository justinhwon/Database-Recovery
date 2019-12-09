package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(hw5): implement

        // get transactionTableEntry if exists
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        // create the commit record
        CommitTransactionLogRecord commitRecord = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(commitRecord);

        // Update lastLSN of transaction table to commit record
        transactionEntry.lastLSN = LSN;

        // change transaction status to committing
        transactionEntry.transaction.setStatus(Transaction.Status.COMMITTING);

        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(hw5): implement

        // get transactionTableEntry if exists
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        // create the abort record
        AbortTransactionLogRecord abortRecord = new AbortTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(abortRecord);

        // Update lastLSN of transaction table to abort record
        transactionEntry.lastLSN = LSN;

        // change transaction status to aborting
        transactionEntry.transaction.setStatus(Transaction.Status.ABORTING);

        // Return abort record
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(hw5): implement

        // get transactionTableEntry if exists
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        // create new end record
        EndTransactionLogRecord endRecord = new EndTransactionLogRecord(transNum, prevLSN);

        // if aborting, undo transaction
        if (transactionEntry.transaction.getStatus() == Transaction.Status.ABORTING){

            // get the latest log in the chain
            LogRecord prevLog = logManager.fetchLogRecord(prevLSN);

            // undo if undoable until prevLSN == 0
            while(prevLog.LSN != 0){

                // if undoable, undo
                if (prevLog.isUndoable()){
                    // get CLR by passing lastLSN of transaction to undo method
                    Pair<LogRecord, Boolean> clr = prevLog.undo(transactionEntry.lastLSN);

                    // add undo-only record to log
                    logManager.appendToLog(clr.getFirst());

                    // Flush log if boolean is True
                    if(clr.getSecond()){
                        logManager.flushToLSN(clr.getFirst().LSN);
                    }

                    // undo the action in the record
                    clr.getFirst().redo(diskSpaceManager, bufferManager);

                    // Update lastLSN of transaction table
                    transactionEntry.lastLSN = clr.getFirst().LSN;

                    // get next LSN to undo
                    Optional<Long> undoNextLSN = clr.getFirst().getUndoNextLSN();

                    // set prevLog to that LSN if exists
                    if(undoNextLSN.isPresent()){
                        prevLog = logManager.fetchLogRecord(undoNextLSN.get());
                    }
                    // if prevLSN doesn't exist, stop undoing
                    else{
                        break;
                    }
                }
                // otherwise just return next prevLog
                else{
                    if(prevLog.getPrevLSN().isPresent()){
                        prevLog = logManager.fetchLogRecord(prevLog.getPrevLSN().get());
                    }
                    // if prevLSN doesn't exist, stop undoing
                    else{
                        break;
                    }
                }



            }
        }

        // once ready to end, append end record to log
        long LSN = logManager.appendToLog(endRecord);

        // Update lastLSN of transaction table to end record
        transactionEntry.lastLSN = LSN;

        // change transaction status to complete
        transactionEntry.transaction.setStatus(Transaction.Status.COMPLETE);

        // remove transaction from transaction table
        transactionTable.remove(transNum);

        // Return end record
        return LSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(hw5): implement

        // get size of update (AFTER)
        int updateSize = after.length;

        // get transactionTableEntry if exists
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        long prevLSN = transactionEntry.lastLSN;

        // check if page exists in dirtyPageTable by getting recLSN (page number -> recLSN).
        Long recLSN = dirtyPageTable.get(pageNum);

        // initialize return value/new lastLSN of transaction
        long LSN;

        // if update size too large, split into undo-only and redo-only update records
        if (updateSize > (BufferManager.EFFECTIVE_PAGE_SIZE / 2)){

            //initialize an empty array
            byte[] emptyArray = new byte[0];

            // first update sets bytes to nothing (undo only)
            UpdatePageLogRecord undoOnlyRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, emptyArray);

            // add undo-only record to log
            logManager.appendToLog(undoOnlyRecord);

            // if page exists, no need to update dirty page table
            // if page doesn't exist, need to update dirty page table
            if(recLSN == null){
                dirtyPageTable.put(pageNum, undoOnlyRecord.LSN);
            }

            // second update sets nothing to correct update (redo only)
            UpdatePageLogRecord redoOnlyRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, emptyArray, after);

            // add redo-only record to log
            LSN = logManager.appendToLog(redoOnlyRecord);

        }
        // otherwise just do one record
        else{
            // create update record if size fits
            UpdatePageLogRecord updateRecord = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);

            // add the new update record to log if size fits
            LSN = logManager.appendToLog(updateRecord);

            // if page exists, no need to update dirty page table
            // if page doesn't exist, need to update dirty page table
            if(recLSN == null){
                dirtyPageTable.put(pageNum, updateRecord.LSN);
            }

        }

        // Update lastLSN of transaction table and touched pages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);

        //return LSN of latest log record added by this update
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(hw5): implement

        // get lastLSN of transactionTableEntry
        long prevLSN = transactionEntry.lastLSN;


        // get the latest log in the chain
        LogRecord prevLog = logManager.fetchLogRecord(prevLSN);

        // undo if undoable until prevLSN <= LSN from savePoint
        while(prevLog.LSN > 0){

            // if undoable, undo
            if (prevLog.isUndoable()){
                // get CLR by passing lastLSN of transaction to undo method
                Pair<LogRecord, Boolean> clr = prevLog.undo(transactionEntry.lastLSN);

                // add undo-only record to log
                logManager.appendToLog(clr.getFirst());

                // Flush log if boolean is True
                if(clr.getSecond()){
                    logManager.flushToLSN(clr.getFirst().LSN);
                }

                // undo the action in the record
                clr.getFirst().redo(diskSpaceManager, bufferManager);

                // Update lastLSN of transaction table
                transactionEntry.lastLSN = clr.getFirst().LSN;

                // get next LSN to undo
                Optional<Long> undoNextLSN = clr.getFirst().getUndoNextLSN();

                // set prevLog to that LSN if exists
                if(undoNextLSN.isPresent()){
                    prevLog = logManager.fetchLogRecord(undoNextLSN.get());
                }
                // if prevLSN doesn't exist, stop undoing
                else{
                    break;
                }
            }
            // otherwise just get next prevLog
            else{
                if(prevLog.getPrevLSN().isPresent()){
                    prevLog = logManager.fetchLogRecord(prevLog.getPrevLSN().get());
                }
                // if prevLSN doesn't exist, stop undoing
                else{
                    break;
                }
            }
        }

        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(hw5): generate end checkpoint record(s) for DPT and transaction table

        // fill in the dirty page table entries for the checkpoint
        for (Map.Entry<Long, Long> entry : dirtyPageTable.entrySet()){

            // get key for dpt hashmap
            Long pageNum = entry.getKey();
            // get value for dpt hashmap
            Long recLSN = entry.getValue();

            // check if end checkpoint log record is full
            boolean fitsAfterAdd;
            fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size()+1, touchedPages.size(), numTouchedPages);

            // start new end checkpoint log record if full
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }

            // put pageNum, recLSN in dpt hashmap (for later input into checkpoint record)
            dpt.put(pageNum, recLSN);

        }


        // fill in the transaction table entries for the checkpoint
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            TransactionTableEntry transactionEntry = entry.getValue();

            // check if end checkpoint log record is full
            boolean fitsAfterAdd;
            fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(dpt.size(), txnTable.size()+1, touchedPages.size(), numTouchedPages);

            // start new end checkpoint log record if full
            if (!fitsAfterAdd) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);

                dpt.clear();
                txnTable.clear();
                touchedPages.clear();
                numTouchedPages = 0;
            }

            // create pair of status, lastLSN
            Pair<Transaction.Status, Long> pairStatusLastLSN = new Pair(transactionEntry.transaction.getStatus(), transactionEntry.lastLSN);
            // put the pair into the txnTable map (for later input into checkpoint record)
            txnTable.put(transNum, pairStatusLastLSN);
        }

        // below here is skeleton code

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(hw5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(hw5): implement
        return () -> {};
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;


        // TODO(hw5): implement

        // list of page transaction operation types
        Set<LogType> pageTransOps = new HashSet<>();
        pageTransOps.add(LogType.ALLOC_PAGE);
        pageTransOps.add(LogType.UPDATE_PAGE);
        pageTransOps.add(LogType.FREE_PAGE);
        pageTransOps.add(LogType.UNDO_UPDATE_PAGE);
        pageTransOps.add(LogType.UNDO_FREE_PAGE);
        pageTransOps.add(LogType.UNDO_ALLOC_PAGE);

        // list of partition transaction operation types
        Set<LogType> partTransOps = new HashSet<>();
        partTransOps.add(LogType.ALLOC_PART);
        partTransOps.add(LogType.FREE_PART);
        partTransOps.add(LogType.UNDO_ALLOC_PART);
        partTransOps.add(LogType.UNDO_FREE_PART);

        // get log iterator
        Iterator<LogRecord> logIter = logManager.scanFrom(LSN);

        // loop until no more log records
        while(logIter.hasNext()){

            // get the log record and its transaction number
            LogRecord logRecord = logIter.next();
            Long transNum = null;
            if(logRecord.getTransNum().isPresent()){
                transNum = logRecord.getTransNum().get();
            }

            // if page-related log record
            if (pageTransOps.contains(logRecord.type)){

                // if transaction not in table, create new transaction and put in table
                if (!transactionTable.containsKey(transNum)){
                    TransactionTableEntry newTransEntry = new TransactionTableEntry(newTransaction.apply(transNum));
                    transactionTable.put(transNum, newTransEntry);
                }
                // update lastLSN to current record
                transactionTable.get(transNum).lastLSN = logRecord.LSN;

                // if page-related, get X lock for that page
                // page needs to be added to the touchedPages set
                if(logRecord.getPageNum().isPresent()){
                    transactionTable.get(transNum).touchedPages.add(logRecord.getPageNum().get());
                    LockContext pageLockContext = getPageLockContext(logRecord.getPageNum().get());
                    acquireTransactionLock(transactionTable.get(transNum).transaction.getTransactionContext(), pageLockContext, LockType.X);
                }

                // if update/undoUpdate then no need to flush immediately
                if(logRecord.type == LogType.UPDATE_PAGE || logRecord.type == LogType.UNDO_UPDATE_PAGE){
                    Long recLSN = dirtyPageTable.get(logRecord.getPageNum().get());
                    if(recLSN == null){
                        dirtyPageTable.put(logRecord.getPageNum().get(), logRecord.LSN);
                    }

                }
                // otherwise flush immediately
                else{
                    logManager.flushToLSN(logRecord.LSN);
                    // remove transaction from dpt since flushed
                    dirtyPageTable.remove(logRecord.getPageNum().get());
                }

            }

            // if partition-related log record
            if (partTransOps.contains(logRecord.type)){

                // if transaction not in table, create new transaction and put in table
                if (!transactionTable.containsKey(transNum)){
                    TransactionTableEntry newTransEntry = new TransactionTableEntry(newTransaction.apply(transNum));
                    transactionTable.put(transNum, newTransEntry);
                }
                // update lastLSN to current record
                transactionTable.get(transNum).lastLSN = logRecord.LSN;

            }



            //CommitTransaction/AbortTransaction/EndTransaction
            // If the log record is for a change in transaction status - COMMIT_TRANSACTION
            if(logRecord.type == LogType.COMMIT_TRANSACTION){
                // get transactionTableEntry of transaction
                TransactionTableEntry commitTransaction = transactionTable.get(transNum);
                // set transaction status to COMMITTING
                commitTransaction.transaction.setStatus(Transaction.Status.COMMITTING);
                // update transaction table
                commitTransaction.lastLSN = logRecord.LSN;

            }
            // If the log record is for a change in transaction status - ABORT_TRANSACTION
            if(logRecord.type == LogType.ABORT_TRANSACTION){
                // get transactionTableEntry of transaction
                TransactionTableEntry abortTransaction = transactionTable.get(transNum);
                // set transaction status to RECOVERY_ABORTING
                abortTransaction.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                // update transaction table
                abortTransaction.lastLSN = logRecord.LSN;

            }
            // If the log record is for a change in transaction status
            // clean up transaction (Transaction#cleanup) if END_TRANSACTION
            if(logRecord.type == LogType.END_TRANSACTION){
                // clean up the transaction if END_TRANSACTION
                TransactionTableEntry endTransaction = transactionTable.get(transNum);
                endTransaction.transaction.cleanup();
                // set transaction status to COMPLETE
                endTransaction.transaction.setStatus(Transaction.Status.COMPLETE);
                // remove transaction from transaction
                transactionTable.remove(transNum);

            }


            // If the log record is a begin_checkpoint record: Update the transaction counter
            if (logRecord.type == LogType.BEGIN_CHECKPOINT ){
                if(logRecord.getMaxTransactionNum().isPresent()){
                    updateTransactionCounter.accept(logRecord.getMaxTransactionNum().get());
                }
            }
            //If the log record is an end_checkpoint record:
            if (logRecord.type == LogType.END_CHECKPOINT){

                // Copy all entries of checkpoint DPT (replace existing entries if any)
                for (Map.Entry<Long, Long> entry : logRecord.getDirtyPageTable().entrySet()) {
                    dirtyPageTable.put(entry.getKey(), entry.getValue());
                }

                //Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
                //add to transaction table if not already present.
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : logRecord.getTransactionTable().entrySet()) {

                    TransactionTableEntry transactionEntry = transactionTable.get(entry.getKey());
                    // if the transaction exists in current transactionTable
                    if(transactionEntry != null){
                        // if lastLSN of checkpoint transX larger than current, update lastLSN and status
                        if(entry.getValue().getSecond() > transactionEntry.lastLSN){
                            transactionEntry.lastLSN = entry.getValue().getSecond();
                            transactionEntry.transaction.setStatus(entry.getValue().getFirst());
                        }
                    }
                    // if transaction doesn't exist in current transactionTable, add it to table
                    else{
                        // create new transaction
                        TransactionTableEntry newTransEntry = new TransactionTableEntry(newTransaction.apply(entry.getKey()));
                        newTransEntry.lastLSN = entry.getValue().getSecond();
                        newTransEntry.transaction.setStatus(entry.getValue().getFirst());
                        // add new transactionTableEntry to transactionTable
                        transactionTable.put(entry.getKey(), newTransEntry);

                    }
                }

                //Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
                //transaction table if the transaction has not finished yet, and acquire X locks.
                for (Map.Entry<Long, List<Long>> entry : logRecord.getTransactionTouchedPages().entrySet()){
                    TransactionTableEntry transactionEntry = transactionTable.get(entry.getKey());
                    // not finished if transaction is still in transactionTable
                    if (transactionEntry != null){
                        // for every page in touchedPages of checkpoint, add to transactionEntry touchedPages and get X locks
                        for (Long pageNum : entry.getValue()){
                            transactionEntry.touchedPages.add(pageNum);
                            LockContext pageLockContext = getPageLockContext(pageNum);
                            acquireTransactionLock(transactionEntry.transaction.getTransactionContext(), pageLockContext, LockType.X);
                        }
                    }
                }

            }
        }

        // once no more log records, end transactions
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()){
            // get transaction status
            Transaction.Status status = entry.getValue().transaction.getStatus();

            //All transactions in the RUNNING state should be moved into the RECOVERY_ABORTING state,
            // and an abort transaction record should be written.
            if (status == Transaction.Status.RUNNING){
                // get transaction and set status to RECOVERY_ABORTING
                Transaction transaction = entry.getValue().transaction;
                transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                // write an abort transaction record to log
                AbortTransactionLogRecord abortRecord = new AbortTransactionLogRecord(transaction.getTransNum(), entry.getValue().lastLSN);
                // once ready to end, append abort record to log
                logManager.appendToLog(abortRecord);
                // update lastLSN to current record
                entry.getValue().lastLSN = abortRecord.LSN;

            }
            //All transactions in the COMMITTING state should be ended (cleanup(),
            // state set to COMPLETE, end transaction record written, and removed from the transaction table).
            if (status == Transaction.Status.COMMITTING){
                // clean up the transaction if END_TRANSACTION
                Transaction transaction = entry.getValue().transaction;
                transaction.cleanup();
                // set transaction status to COMPLETE
                transaction.setStatus(Transaction.Status.COMPLETE);
                // write an end transaction record
                EndTransactionLogRecord endRecord = new EndTransactionLogRecord(transaction.getTransNum(), entry.getValue().lastLSN);
                // once ready to end, append end record to log
                logManager.appendToLog(endRecord);
                // remove transaction from transaction table
                transactionTable.remove(transaction.getTransNum());

            }
            //Nothing needs to be done for transactions in the RECOVERY_ABORTING state.
            if (status == Transaction.Status.RECOVERY_ABORTING){
                // do nothing
            }
        }

    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(hw5): implement
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(hw5): implement
        return;
    }

    // TODO(hw5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
