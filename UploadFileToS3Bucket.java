import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import java.io.File;
import java.util.Date;
import java.util.List;

public class UploadFileToS3Bucket {

    private static final String accessKeyID = "AKIAIOSFODNN7EXAMPLE"; // replace with actual access Key ID
    private static final String secretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"; // replace with secret Access Key
    private static final String localFilePath = "/path/to/data.json"; // local filepath where data.json file is generated
    private static final String bucketName = "<Target S3 Bucket Name>";
    private static final String targetKeyName = "/path/to/data.json"; // S3 filepath where data.json file is hosted
    
    public static void main(String[] args) throws Exception {
        outputConsoleLogsBreakline("");
        outputConsoleLogsBreakline("Establishing connection with Amazon Web Service (AWS)");
        outputConsoleLogsBreakline("");
        AWSCredentials credentials = new BasicAWSCredentials(accessKeyID, secretAccessKey);
        Regions clientRegion = Regions.AP_SOUTHEAST_1; // Asia Pacific (Singapore) ap-southeast-1
        outputConsoleLogsBreakline("Region: " + clientRegion);
        outputConsoleLogsBreakline("Region name: " + clientRegion.getName());
        outputConsoleLogsBreakline("Region description: " + clientRegion.getDescription());
        outputConsoleLogsBreakline("");
        try {
            outputConsoleLogsBreakline("AWS connection established.");
            outputConsoleLogsBreakline("");
            outputConsoleLogsBreakline("Establishing Amazon S3 Client");
            AmazonS3 s3Client = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(new AWSStaticCredentialsProvider(credentials))
                    .withRegion(clientRegion.getName())
                    .build();

            outputConsoleLogsBreakline("Amazon S3 Client established");
            outputConsoleLogsBreakline("");
            outputConsoleLogsBreakline("Listing all s3 Buckets");
            outputConsoleLogsBreakline("");

            List<Bucket> buckets = s3Client.listBuckets();
            int counter = 1;
            for (Bucket bucket : buckets) {
                System.out.println(("Bucket " + (counter++)) + ": " + bucket.getName());
            }
            outputConsoleLogsBreakline("");

            outputConsoleLogsBreakline("Verifying if bucket named: " + bucketName + " exists");
            if (s3Client.doesBucketExist(bucketName)) {
                System.out.println(bucketName + " S3 bucket exists.");
                outputConsoleLogsBreakline("");
                outputConsoleLogsBreakline("Listing Objects in the s3 bucket named: " + bucketName);

                outputConsoleLogsBreakline("");
                outputConsoleLogsBreakline("Listing all Objects in Bucket: " + bucketName);
                outputConsoleLogsBreakline("");
                ObjectListing objectListing = s3Client.listObjects(bucketName);

                String keyName = null;
                Date lastModified = null;
                for (S3ObjectSummary os : objectListing.getObjectSummaries()) {
                    keyName = os.getKey();
                    lastModified = os.getLastModified();
                    System.out.println("Object: " + keyName + " | Last modified at: " + lastModified);
                }
                outputConsoleLogsBreakline("");
                File fileToReplace = new File(localFilePath);

                TransferManager tm = TransferManagerBuilder.standard()
                        .withS3Client(s3Client)
                        .build();

                System.out.println("TransferManager processes all transfers asynchronously.");
                Upload upload = tm.upload(bucketName, targetKeyName, fileToReplace);
                outputConsoleLogsBreakline("Object upload started");
                upload.waitForCompletion();
                System.out.println("---****************--->" + s3Client.getUrl(bucketName, targetKeyName).toString());
                upload.waitForCompletion();
                outputConsoleLogsBreakline("--*---> " + s3Client.getUrl(bucketName, targetKeyName).toString());
                outputConsoleLogsBreakline("--**-> " + upload.getProgress());
                outputConsoleLogsBreakline("--***---> " + s3Client.getUrl(bucketName, targetKeyName).toString());
                outputConsoleLogsBreakline("--****-> " + upload.getState());
                outputConsoleLogsBreakline("--*****---> " + s3Client.getUrl(bucketName, targetKeyName).toString());
                outputConsoleLogsBreakline("--******-> 0");
                outputConsoleLogsBreakline("Object upload complete");
                outputConsoleLogsBreakline("");
                outputConsoleLogsBreakline("Shutting down TransferMananger");
                tm.shutdownNow();
                outputConsoleLogsBreakline("");
            } else {
                System.out.println(bucketName + " S3 bucket is not available. Please try again with a different Bucket name.");
            }
            outputConsoleLogsBreakline("");
        } catch (AmazonServiceException e) {
            outputConsoleLogsBreakline("[AmazonServiceException] The call was transmitted successfully");
            outputConsoleLogsBreakline("[AmazonServiceException] but Amazon S3 couldn't process it.");
            e.printStackTrace();
        } catch (SdkClientException e) {
            outputConsoleLogsBreakline("[SdkClientException] Amazon S3 couldn't be contacted for a response.");
            outputConsoleLogsBreakline("[SdkClientException] Client couldn't parse the response from Amazon S3.");
            e.printStackTrace();
        } catch (Exception e) {
            outputConsoleLogsBreakline("[Exception]");
            e.printStackTrace();
        }
        System.out.println("");
    }

    // Utility method to output status of upload
    private static void outputConsoleLogsBreakline(String consoleCaption) {
        String logString = "";

        int charLimit = 100;
        if (consoleCaption.length() > charLimit) {
            logString = consoleCaption.substring(0, charLimit - 4) + " ...";
        } else {
            String result = "";

            if (consoleCaption.isEmpty()) {
                for (int i = 0; i < charLimit; i++) {
                    result += "=";
                }
                logString = result;
            } else {
                charLimit = (charLimit - consoleCaption.length() - 1);
                for (int i = 0; i < charLimit; i++) {
                    result += "-";
                }
                logString = consoleCaption + " " + result;
            }
        }
        System.out.println(logString);
    }
}
