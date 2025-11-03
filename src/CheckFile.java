import java.io.*;

/**
 * CheckFile: Check to see if a file is sorted. This assumes that each record is
 * a pair of 32-bit ints with the first int being the key value.
 * This also assumes that the data values were selected to let CheckFile verify
 * that the sort is stable (so the data values of equal keys must be increasing)
 *
 * @author CS3114 Instructor and TAs
 * @version Fall 2025
 */

public class CheckFile
{
    /**
     * This is an empty constructor for a CheckFile object.
     */
    public CheckFile()
    {
        // empty constructor
    }


    /**
     * This method checks a file to see if it is properly sorted by key value.
     *
     * @param filename
     *            a string containing the name of the file to check
     * @return true if the file is sorted, false otherwise
     * @throws Exception
     *             either an IOException or a FileNotFoundException
     */
    public boolean checkFile(String filename)
        throws Exception
    {
        boolean isError = false;
        try (DataInputStream in =
             new DataInputStream(new BufferedInputStream(new FileInputStream(
            filename)))) {
            // Prime with the first record
            int key2 = in.readInt();
            int data = in.readInt();
            if ((key2 <= 0) || (data <= 0)) {
                isError = true;
            }
            int reccnt = 0;
            try
            {
                while (true)
                {
                    int key1 = key2;
                    reccnt++;
                    key2 = in.readInt();
                    data = in.readInt();
                    if ((key2 <= 0) || (data <= 0)) {
                        isError = true;
                    }
                    if (key1 > key2)
                    {
                        isError = true;
                    }
                }
            }
            catch (EOFException e)
            {
                System.out.println(reccnt + " records processed");
            }
            in.close();
        }
        return !isError;
    }


    /**
     * This method checks a file to see if it is properly sorted by key value.
     * This version requires that the data fields of records with equal key
     * values are also in non-descending order. Used to verify that the
     * implemented sort is stable if the input file is built with ascending
     * data field values (but is game-able if the implementor uses
     * the data field as part of the sort key).
     * So, we don't rely on this for doing reference tests.
     *
     * @param filename
     *            a string containing the name of the file to check
     * @return true if the file is sorted, false otherwise
     * @throws Exception
     *             either an IOException or a FileNotFoundException
     */
    public boolean checkFileStrong(String filename)
        throws Exception
    {
        boolean isError = false;
        try (DataInputStream in =
             new DataInputStream(new BufferedInputStream(new FileInputStream(
            filename)))) {
            // Prime with the first record
            int key2 = in.readInt();
            int data2 = in.readInt();
            if ((key2 <= 0) || (data2 <= 0)) {
                isError = true;
            }
            int reccnt = 0;
            try
            {
                while (true)
                {
                    int key1 = key2;
                    int data1 = data2;
                    reccnt++;
                    key2 = in.readInt();
                    data2 = in.readInt();
                    if ((key2 <= 0) | (data2 <= 0)) {
                        isError = true;
                    }
                    if (key1 > key2)
                    {
                        isError = true;
                    }
                    if (key1 == key2) {
                        if (data1 > data2) {
                            isError = true;
                        }
                    }
                }
            }
            catch (EOFException e)
            {
                System.out.println(reccnt + " records processed");
            }
            in.close();
        }
        return !isError;
    }
}
