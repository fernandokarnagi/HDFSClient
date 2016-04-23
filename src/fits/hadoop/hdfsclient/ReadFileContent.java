package fits.hadoop.hdfsclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ReadFileContent {

	public static void main(String[] args) {
		try {
			System.out.println("Configuring hadoop environment");
			Configuration conf = new Configuration();
			String hadoopPath = "/usr/local/Cellar/hadoop/2.7.2/libexec/etc/hadoop/";
			conf.addResource(new Path(hadoopPath + "core-site.xml"));
			conf.addResource(new Path(hadoopPath + "hdfs-site.xml"));
			conf.addResource(new Path(hadoopPath + "mapred-site.xml"));

			FileSystem fileSystem = FileSystem.get(conf);
			readFile("/user/fernando/fernand-readme.txt", fileSystem);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void readFile(String dest, FileSystem fileSystem) throws IOException {

		System.out.println("Read file content: " + dest);

		try {
			InputStream is = fileSystem.open(new Path(dest));
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String ln = br.readLine();
			System.out.println(ln);

			is.close();

			System.out.println("File has been read");

		} catch (Exception e) {
			System.err.println("Exception caught! :" + e);
			System.exit(1);
		} finally {
			fileSystem.close();
		}
	}
}
