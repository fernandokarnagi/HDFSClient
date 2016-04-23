package fits.hadoop.hdfsclient;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class AddFile {

	public static void main(String[] args) {
		try {
			System.out.println("Configuring hadoop environment");
			Configuration conf = new Configuration();
			String hadoopPath = "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/";
			conf.addResource(new Path(hadoopPath + "core-site.xml"));
			conf.addResource(new Path(hadoopPath + "hdfs-site.xml"));
			conf.addResource(new Path(hadoopPath + "mapred-site.xml"));

			FileSystem fileSystem = FileSystem.get(conf);
			addFile("/user/fernando/fernand-readme.txt", fileSystem);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void addFile(String dest, FileSystem fileSystem) throws IOException {

		System.out.println("Add file into destination: " + dest);

		try {
			boolean overwrite = true;
			OutputStream os = fileSystem.create(new Path(dest), overwrite);
			PrintWriter ps = new PrintWriter(os);
			ps.print("Fernando just added new content");
			ps.flush();
			ps.close();
			System.out.println("File has been created");

			fileSystem.mkdirs(new Path("/user/fernando/newfolder"));
			System.out.println("Folder has been created");
		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}
}
