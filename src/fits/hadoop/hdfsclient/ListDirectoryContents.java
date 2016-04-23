package fits.hadoop.hdfsclient;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class ListDirectoryContents {

	public static void main(String[] args) {
		try {
			System.out.println("Configuring hadoop environment");
			Configuration conf = new Configuration();
			String hadoopPath = "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/";
			conf.addResource(new Path(hadoopPath + "core-site.xml"));
			conf.addResource(new Path(hadoopPath + "hdfs-site.xml"));
			conf.addResource(new Path(hadoopPath + "mapred-site.xml"));

			FileSystem fileSystem = FileSystem.get(conf);
			listFolder("/user/fernando/", fileSystem);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void listFolder(String folder, FileSystem fileSystem) throws IOException {

		System.out.println("List folder: " + folder);

		try {
			boolean recursive = true;
			RemoteIterator<LocatedFileStatus> filesIt = fileSystem.listFiles(new Path(folder), recursive);
			while (filesIt.hasNext()) {
				LocatedFileStatus fsStatus = filesIt.next();
				Path file = fsStatus.getPath();
				System.out.println(file);
			}
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}
}
