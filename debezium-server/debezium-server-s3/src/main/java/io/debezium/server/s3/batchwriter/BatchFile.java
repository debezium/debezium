package io.debezium.server.s3.batchwriter;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

class BatchFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchFile.class);
    private int numRecords = 0;
    private int batchId = 0;
    private FileOutputStream fileStream = null;
    private File batchFile = null;

    public BatchFile(File batchFile) throws FileNotFoundException {
        this.setBatchFile(batchFile);
    }

    public void setBatchFile(File batchFile) throws FileNotFoundException {
        LOGGER.debug("Creating file " + batchFile.getAbsolutePath().toLowerCase());
        this.batchFile = batchFile;
        batchFile.getParentFile().mkdirs();
        this.fileStream = new FileOutputStream(batchFile, true);
        this.numRecords = 0;
        this.batchId++;
    }

    public Path getAbsolutePath() {
        return Paths.get(this.batchFile.getAbsolutePath());
    }

    public int getNumRecords() {
        return numRecords;
    }

    public int getBatchId() {
        return batchId;
    }

    public void append(String data) throws IOException {
        IOUtils.write(data + IOUtils.LINE_SEPARATOR, fileStream, Charset.defaultCharset());
        numRecords++;
    }

    public void close() throws IOException {
        fileStream.close();
    }
}
