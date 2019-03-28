package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.block.stream.BlockInStream;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.examples.BasicOperations;
import alluxio.exception.AlluxioException;
import alluxio.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import alluxio.master.file.
//import alluxio.master.file.meta.LockedInodePath;

import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by renfei on 2017/11/24.
 * <p>
 * This class reads the file in parallel from k machines and assemble them into a single buffer.
 */
public class SPFileReader {

    private int mK;

    private FileSystem mFileSystem;

    private AlluxioURI mFilePath;

    private OpenFileOptions mReadOptions;

    private static final Logger LOG = LoggerFactory.getLogger(SPFileReader.class);

    private final String mLog; // for cache hits in file level


    public SPFileReader(FileSystem fileSystem, AlluxioURI filePath) throws
            IOException,
            AlluxioException {
        mFileSystem = fileSystem;
        mFilePath = filePath;
        mReadOptions = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
        mReadOptions.setForSP(true);
        mK = mFileSystem.getStatus(filePath).getKValueForSP();
        mLog = System.getProperty("user.dir") + "/logs/fileHit.txt"; // log the popularity
    }

    public void setReadOption(OpenFileOptions readOptions) {
        mReadOptions = readOptions;
    }

    public long runRead() throws Exception {
        // Initialize a buffer for reading
        FileInStream is = mFileSystem.openFile(mFilePath, mReadOptions);
        // LOG.info("yinghao debug:"+is.mStatus.getInMemoryPercentage());
        FileWriter fw = new FileWriter(mLog, true); //the true will append the new data
        int cachedPercentage = is.mStatus.getInMemoryPercentage();
        long fileSize = is.mStatus.getLength();
        fw.write("" + cachedPercentage + "\t" + fileSize + "\n");
        fw.flush();
        byte[] fileBuf = new byte[(int) is.mFileLength];

        ExecutorService executorService = Executors.newCachedThreadPool();

        //LockedInodePath inodePath = mInodeTree.lockInodePath(path, InodeTree.LockMode.WRITE)
        final long startTimeMs = CommonUtils.getCurrentMs();
        fw.write("Now enter executorService.execute, mK is " + mK + "\n");
        fw.flush();
        for (int i = 0; i < mK; i++) {
            try {
                fw.write("Now enter executorService.execute, i:" + i + "\n");
                fw.flush();
                executorService.execute(new ReadBlockThread(
                        fileBuf,
                        mFilePath,
                        mFileSystem,
                        mReadOptions,
                        i
                ));
            }catch (Exception e){
                fw.write(e.toString()+"\n");
                fw.flush();
            }
        }
        fw.write("Now leave executorService.executor\n");
        fw.flush();
        executorService.shutdown();
        fw.write("executorService.shutdown() finished\n");
        fw.flush();
        try {
            executorService.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // Thread.sleep(5000L); //debug
        final long endTimeMs = CommonUtils.getCurrentMs();
        fw.write(mFilePath.getPath() + "read spend time: " + (endTimeMs - startTimeMs) + "\n");
        fw.flush();
        fw.close();
        return endTimeMs - startTimeMs;
    }

}
