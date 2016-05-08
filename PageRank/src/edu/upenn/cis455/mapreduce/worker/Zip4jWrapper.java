package edu.upenn.cis455.mapreduce.worker;

import java.io.File;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

/**
 * Created by Bill Dong on 4/22/16.
 */
public class Zip4jWrapper {

    public boolean zip(File folderToZip, File fileOfZip) {
        try {
            ZipFile zipFile = new ZipFile(fileOfZip);
            ZipParameters parameters = new ZipParameters();
            parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE);
            parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);
            zipFile.createZipFileFromFolder(folderToZip, parameters, false, 0);
        } catch (ZipException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public boolean unzip(File fileToUnzip, File folderOfUnzip) {
        try {
            ZipFile zipFile = new ZipFile(fileToUnzip);
            zipFile.extractAll(folderOfUnzip.getPath());
        } catch (ZipException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }
}