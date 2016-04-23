package fits.hadoop.hdfsclient;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CopyToLocal {

	public static void main(String[] args) {
		try {
			System.out.println("Configuring hadoop environment");
			Configuration conf = new Configuration();
			String hadoopPath = "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/";
			conf.addResource(new Path(hadoopPath + "core-site.xml"));
			conf.addResource(new Path(hadoopPath + "hdfs-site.xml"));
			conf.addResource(new Path(hadoopPath + "mapred-site.xml"));

			FileSystem fileSystem = FileSystem.get(conf);
			copyToLocal("/user/fernando/fernando.png",
					"/Users/fernando/Work/Apps/Hadoop/hdfsclient/fernando-from-hdfs.png", fileSystem);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void copyToLocal(String source, String dest, FileSystem fileSystem) throws IOException {

		System.out.println("Copying from HDFS source: " + source + ", local destination: " + dest);
		Path srcPath = new Path(source);
		Path dstPath = new Path(dest);

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1, source.length());

		try {
			fileSystem.copyToLocalFile(srcPath, dstPath);
			System.out.println("File " + filename + " has been copied to " + dest);
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}

}
