import java.nio.*;
import java.io.*;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

// The Radix Sort implementation
// -------------------------------------------------------------------------
/**
 *
 * @author Ben Blucher, Austin Kane (benblucher, austink23)
 * @version 10.21.2025
 */
public class Radix {

    private static final int KEY_SIZE = 4; // 4-byte integer key
    private static final int RECORD_SIZE = 8; // 8-byte record (key + data)
    private static final int MEMORY_POOL_SIZE = 900000; // 900,000 bytes working
                                                        // memory [cite: 15]
    private static final int RADIX = 8; // Number of bits to process per pass
    private static final int K = 4; // Number of passes for a 32-bit key (32 / 8
                                    // = 4)
    private static final int R = 1 << RADIX; // Number of buckets (2^8 = 256)

    // --- I/O and Statistics Fields ---
    private RandomAccessFile inputFile; // The file to read from (starts as
                                        // original file)
    private RandomAccessFile outputFile; // The file to write to (starts as temp
                                         // file)
    private PrintWriter statsWriter; // For outputting statistics [cite: 53]
    private long fileSize;
    private long numRecords;
    private long numReads; // Number of disk reads [cite: 59]
    private long numWrites; // Number of disk writes [cite: 60]

    // --- Memory Pool and Buffer Management ---
    private byte[] memoryPool; // The 900,000 byte array [cite: 15]
    private ByteBuffer byteBuffer; // Wrapper for the memoryPool for I/O
    private RandomAccessFile tempFile; // The actual temporary file handle
    private String tempFileName = "tempfile.bin"; // Name for the temporary file

    // --- Buffer Sizing ---
    // We need 1 input buffer and R (256) output buffers.
    private static final int NUM_BUFFERS = R + 1; // 257 buffers

    // Calculate size of each buffer. Must be a multiple of RECORD_SIZE.
    private static final int BUFFER_RECORDS = (MEMORY_POOL_SIZE / NUM_BUFFERS)
        / RECORD_SIZE;
    private static final int BUFFER_SIZE = BUFFER_RECORDS * RECORD_SIZE;

    // --- Buffer References ---
    private ByteBuffer inputBuffer;
    private ByteBuffer[] outputBuffers;

    // To copy records from input to output without allocating new byte[]
    private byte[] tempRecord = new byte[RECORD_SIZE];

    private static final int HALF_POOL_SIZE = MEMORY_POOL_SIZE / 2;

    /**
     * Create a new Radix object.
     * 
     * @param theFile
     *            The RandomAccessFile to be sorted
     * @param s
     *            The stats PrintWriter
     *
     * @throws IOException
     */
    public Radix(RandomAccessFile theFile, PrintWriter s) throws IOException {
        this.statsWriter = s;
        this.inputFile = theFile;
        this.fileSize = theFile.length();
        this.numRecords = this.fileSize / RECORD_SIZE;
        this.numReads = 0;
        this.numWrites = 0;
        this.outputBuffers = new ByteBuffer[R];

        // 1. Initialize Memory Pool
        this.memoryPool = new byte[MEMORY_POOL_SIZE];
        this.inputBuffer = ByteBuffer.wrap(memoryPool, 0, BUFFER_SIZE);

        // Output buffers are the next R blocks
        for (int i = 0; i < R; i++) {
            int offset = (i + 1) * BUFFER_SIZE;
            this.outputBuffers[i] = ByteBuffer.wrap(memoryPool, offset,
                BUFFER_SIZE);
        }

        // 2. Create and Open Temporary File
        // The temp file's initial size should be 0, but it will grow to the
        // size of the input file.
        tempFile = new RandomAccessFile(tempFileName, "rw");
        tempFile.setLength(fileSize); // Pre-allocate the space to match the
                                      // input file size
        this.outputFile = tempFile;

        // 3. Write Initial Statistics
        // RadixProj.java already writes the file name and size.
        statsWriter.println("Memory pool: 1 input buffer, " + R
            + " output buffers");
        statsWriter.println("Buffer size: " + BUFFER_SIZE + " bytes ("
            + BUFFER_RECORDS + " records)");

        // 4. Start Sort
        radixSort();

        // 5. Clean up temporary file (Crucial!)
        tempFile.close();
        Files.delete(Paths.get(tempFileName));
    }

    /**
     * Do a Radix sort
     *
     * @throws IOException
     */
    /*
     * private void radixSort() throws IOException {
     * int[] count = new int[R]; // Count[i] stores number of records for bin i
     * 
     * // Buffers for I/O
     * // ByteBuffer inputBlock = ByteBuffer.wrap(memoryPool, 0,
     * HALF_POOL_SIZE);
     * // ByteBuffer outputBlock = ByteBuffer.wrap(memoryPool, HALF_POOL_SIZE,
     * // HALF_POOL_SIZE);
     * 
     * // Loop for K passes (4 passes for 32-bit key with 8-bit radix)
     * for (int pass = 0; pass < K; pass++) {
     * // --- Phase 1: Count Records and Calculate Prefix Sum ---
     * 
     * Arrays.fill(count, 0);
     * inputFile.seek(0);
     * long bytesRemaining = fileSize;
     * 
     * // 1.1. Count the number of records for each bin (using the first
     * // HALF_POOL_SIZE for read buffer)
     * while (bytesRemaining > 0) {
     * int readSize = (int)Math.min(bytesRemaining, HALF_POOL_SIZE);
     * 
     * inputBuffer.clear();
     * 
     * // Read block from disk
     * // We use readFully for reliable block reads (it handles EOF
     * // checks if file size is correct)
     * inputFile.readFully(memoryPool, 0, readSize);
     * numReads++; // Count a disk read [cite: 59]
     * 
     * inputBuffer.limit(readSize);
     * IntBuffer intBuffer = inputBuffer.asIntBuffer();
     * 
     * int rtok = pass * RADIX;
     * int mask = R - 1;
     * 
     * for (int i = 0; i < readSize / RECORD_SIZE; i++) {
     * int key = intBuffer.get(2 * i);
     * int digit = (key >> rtok) & mask;
     * count[digit]++;
     * }
     * bytesRemaining -= readSize;
     * }
     * 
     * // 1.2. Prefix Sum: Calculate starting byte position in the output
     * // file
     * long currentTotal = 0;
     * long[] destBytePosition = new long[R];
     * for (int j = 0; j < R; j++) {
     * destBytePosition[j] = currentTotal; // Store the starting byte
     * // position
     * currentTotal += (long)count[j] * RECORD_SIZE;
     * }
     * 
     * // --- Phase 2: Distribution Pass (Read from Input, Write to Output)
     * // ---
     * 
     * long[] bucketWritePositions = Arrays.copyOf(destBytePosition, R);
     * 
     * // Clear all output buffers before starting
     * for (ByteBuffer buffer : outputBuffers) {
     * buffer.clear();
     * }
     * 
     * inputFile.seek(0);
     * bytesRemaining = fileSize;
     * 
     * // This array will hold a small number of records to be written to
     * // the output buffer
     * // Note: We MUST write to the destination *position*, so we can't
     * // use a simple sequential write.
     * // The most efficient and stable method requires a dedicated output
     * // block for each bucket,
     * // which exceeds the size of 'count' and is a huge $R$-way
     * // merge/buffer-pool design.
     * 
     * // **Compromise:** Use a small, fixed-size cache of output blocks to
     * // reduce excessive seeking.
     * // A single record write per bucket is necessary if we stick to the
     * // provided structure.
     * 
     * // *Reverting to the previous logic but ensuring full block reads
     * // for correctness*
     * 
     * while (bytesRemaining > 0) {
     * int readSize = (int)Math.min(bytesRemaining, HALF_POOL_SIZE);
     * 
     * inputBuffer.clear();
     * inputFile.readFully(memoryPool, 0, readSize);
     * numReads++; // Read of input block
     * 
     * inputBuffer.limit(readSize);
     * IntBuffer intBuffer = inputBuffer.asIntBuffer();
     * 
     * IntBuffer intBuffer = inputBuffer.asIntBuffer();
     * int rtok = pass * RADIX;
     * int mask = R - 1;
     * 
     * // Process each record in the current buffer
     * for (int i = 0; i < readSize / RECORD_SIZE; i++) {
     * int key = intBuffer.get(2 * i);
     * int digit = (key >> rtok) & mask;
     * 
     * // Get the 8-byte record from the input buffer
     * // byte[] record = new byte[RECORD_SIZE];
     * // inputBlock.position(i * RECORD_SIZE);
     * // inputBlock.get(record);
     * 
     * // If the target output buffer is full, flush it to disk
     * if (!outputBuffers[digit].hasRemaining()) {
     * flushOutputBuffer(digit, bucketWritePositions);
     * }
     * 
     * // Write the record to its destination in the output file
     * // outputFile.seek(destBytePosition[digit]);
     * // outputFile.write(record);
     * // numWrites++; // Unoptimized write-per-record - REMAINS A
     * // CRITICAL PERFORMANCE ISSUE
     * 
     * // Copy the 8-byte record from the input buffer
     * // section of the pool to the output buffer section.
     * int recordOffset = i * RECORD_SIZE;
     * outputBuffers[digit].put(memoryPool, recordOffset,
     * RECORD_SIZE);
     * 
     * // Update the next write position for this bucket
     * destBytePosition[digit] += RECORD_SIZE;
     * }
     * bytesRemaining -= readSize;
     * }
     * 
     * // --- Phase 2.5: Final Flush ---
     * // Flush all remaining data from output buffers to disk
     * for (int i = 0; i < R; i++) {
     * if (outputBuffers[i].position() > 0) {
     * outputFile.seek(bucketWritePositions[i]);
     * outputFile.write(outputBuffers[i].array(), outputBuffers[i]
     * .arrayOffset(), outputBuffers[i].position());
     * numWrites++;
     * }
     * }
     * 
     * outputFile.getChannel().force(true); // Force writes to disk
     * 
     * // --- Phase 3: Swap Input and Output Files ---
     * // Swap file roles to avoid copying B back to A
     * swapFiles();
     * }
     * 
     * swapFiles();
     * // Now the original file (the one that `RadixProj` keeps open) is
     * // `inputFile`, and it holds the sorted data.
     * 
     * // Write final statistics
     * statsWriter.println("Num Disk Reads: " + numReads);
     * statsWriter.println("Num Disk Writes: " + numWrites);
     * }
     */


    /**
     * Do a Radix sort
     *
     * @throws IOException
     */
    private void radixSort() throws IOException {
        int[] count = new int[R]; // Count[i] stores number of records for bin i
        long[] destBytePosition = new long[R]; // Starting byte pos for each
                                               // bucket

        // Loop for K passes (4 passes for 32-bit key with 8-bit radix)
        for (int pass = 0; pass < K; pass++) {

            // --- Phase 1: Count Records and Calculate Prefix Sum ---
            Arrays.fill(count, 0);
            inputFile.seek(0);
            long bytesRemaining = fileSize;

            // 1.1. Count the number of records for each bin
            while (bytesRemaining > 0) {
                // Use BUFFER_SIZE and the class inputBuffer
                int readSize = (int)Math.min(bytesRemaining, BUFFER_SIZE);
                inputBuffer.clear();

                // Read block from disk into the input buffer's section of the
                // pool
                // (index 0)
                inputFile.readFully(memoryPool, 0, readSize);
                numReads++; // Count a disk read
                inputBuffer.limit(readSize);

                IntBuffer intBuffer = inputBuffer.asIntBuffer();
                int rtok = pass * RADIX;
                int mask = R - 1;

                for (int i = 0; i < readSize / RECORD_SIZE; i++) {
                    int key = intBuffer.get(2 * i);
                    int digit = (key >> rtok) & mask;
                    count[digit]++;
                }
                bytesRemaining -= readSize;
            }

            // 1.2. Prefix Sum: Calculate starting byte position in the output
            // file
            long currentTotal = 0;
            for (int j = 0; j < R; j++) {
                destBytePosition[j] = currentTotal; // Store the starting byte
                                                    // position
                currentTotal += (long)count[j] * RECORD_SIZE;
            }

            // --- Phase 2: Distribution Pass (Read, Buffer, Write) ---

            // This array tracks the *next* disk write position for each bucket
            long[] bucketWritePositions = Arrays.copyOf(destBytePosition, R);

            // Clear all output buffers before starting
            for (ByteBuffer buffer : outputBuffers) {
                buffer.clear();
            }

            inputFile.seek(0);
            bytesRemaining = fileSize;

            while (bytesRemaining > 0) {
                // Use BUFFER_SIZE and the class inputBuffer
                int readSize = (int)Math.min(bytesRemaining, BUFFER_SIZE);
                inputBuffer.clear();

                // Read block from disk into the input buffer's section
                inputFile.readFully(memoryPool, 0, readSize);
                numReads++; //
                inputBuffer.limit(readSize);

                IntBuffer intBuffer = inputBuffer.asIntBuffer();
                int rtok = pass * RADIX;
                int mask = R - 1;

                // Process each record in the current input buffer
                for (int i = 0; i < readSize / RECORD_SIZE; i++) {
                    int key = intBuffer.get(2 * i);
                    int digit = (key >> rtok) & mask;

                    // If the target output buffer is full, flush it to disk
                    if (!outputBuffers[digit].hasRemaining()) {
                        flushOutputBuffer(digit, bucketWritePositions);
                    }

                    // Copy the 8-byte record from the input buffer
                    // section of the pool to the output buffer section.
                    int recordOffset = i * RECORD_SIZE;
                    outputBuffers[digit].put(memoryPool, recordOffset,
                        RECORD_SIZE);
                }
                bytesRemaining -= readSize;
            }

            // --- Phase 2.5: Final Flush ---
            // Flush all remaining data from output buffers to disk
            for (int i = 0; i < R; i++) {
                if (outputBuffers[i].position() > 0) {
                    outputFile.seek(bucketWritePositions[i]);
                    outputFile.write(outputBuffers[i].array(), outputBuffers[i]
                        .arrayOffset(), outputBuffers[i].position());
                    numWrites++; //
                }
            }

            outputFile.getChannel().force(true); // Force writes to disk

            // --- Phase 3: Swap Input and Output Files ---
            // Swap file roles to avoid copying B back to A
            swapFiles();
        }

        // The final sorted data is in the `outputFile` (which is the temp
        // file).
        // We swap one last time so `inputFile` (the original file) has the
        // sorted data, as required by RadixProj.
        // swapFiles();

        // Write final statistics
        statsWriter.println("Num Disk Reads: " + numReads);
        statsWriter.println("Num Disk Writes: " + numWrites);
    }


    /**
     * Flushes a specific output buffer to its correct location on disk.
     * 
     * @param bucketIndex
     *            The index of the buffer (0-255) to flush.
     * @param bucketWritePositions
     *            The array tracking the next write position.
     * @throws IOException
     */
    private void flushOutputBuffer(int bucketIndex, long[] bucketWritePositions)
        throws IOException {

        ByteBuffer buffer = outputBuffers[bucketIndex];

        // Seek to the correct write position for this bucket
        outputFile.seek(bucketWritePositions[bucketIndex]);

        // Write the entire buffer's contents
        outputFile.write(buffer.array(), buffer.arrayOffset(), BUFFER_SIZE);
        numWrites++; // Count one disk write [cite: 61]

        // Update the next write position for this bucket
        bucketWritePositions[bucketIndex] += BUFFER_SIZE;

        // Reset the buffer
        buffer.clear();
    }


    private void swapFiles() {
        RandomAccessFile temp = inputFile;
        inputFile = outputFile;
        outputFile = temp;
    }
}
