import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;


// -------------------------------------------------------------------------
/**
 *  Some utility methods to help with sorting and testing the results.
 *
 *  @author CS3114/5040 Staff
 *  @version Fall 2025
 */
public class SortUtils
{


/**
     * Copy a file
     * @param sourcePath source path
     * @param destinationPath dest path
     * @throws IOException
     */
    public static void copyFile(String sourcePath, String destinationPath)
        throws IOException {
        Files.copy(Paths.get(sourcePath),
            new FileOutputStream(destinationPath));
    }

    // ----------------------------------------------------------
    /**
     * Compare that two files are identical
     * Can't use Java Files::mismatch because that is in Java 12,
     * and all the alternatives that I've found boil down to reading and
     * comparing the contents something like this.
     * @param file1 First file
     * @param file2 Second file
     * @return True iff they are identical
     * @throws IOException
     */
    public static boolean diffFile(String file1, String file2)
        throws IOException {
        try (
            BufferedInputStream fis1 =
                new BufferedInputStream(new FileInputStream(new File(file1)));
            BufferedInputStream fis2 =
                new BufferedInputStream(new FileInputStream(new File(file2))))
        {
            int ch = 0;
            while ((ch = fis1.read()) != -1) {
                if (ch != fis2.read()) {
                    return false;
                }
            }
            return fis2.read() == -1;
        }
    }
}
