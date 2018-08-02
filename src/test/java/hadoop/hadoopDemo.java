package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Stack;

/**
 * Created by MyPC on 2018/7/30.
 */
public class hadoopDemo {

    private  FileSystem fileSystem;
    private Path homeDirectory;

    @Before
    public void connectHadoop() throws Exception {
        System.setProperty("hadoop.home.dir","D:/tool/dcp/hadoop/hadoop-2.7.4");
        Configuration con = new Configuration();
        fileSystem = FileSystem.get(new URI("hdfs://192.168.86.129:9000"),con);
        homeDirectory = fileSystem.getHomeDirectory();
    }
    @Test
    //创建根目录
    public void demo1() throws Exception {
       // fileSystem.deleteOnExit(new Path(homeDirectory+"/logs"));
        boolean mkdirs = fileSystem.mkdirs(new Path(homeDirectory+"/logs"));
        System.out.println(mkdirs);
    }
    //查询目录
    @Test
    public void selectDic() throws IOException {
        bianli(homeDirectory);
    }
    //上传文件
    @Test
    public void uploadFile()throws Exception{
        fileSystem.copyFromLocalFile(false,true,new Path("D:/logs/test2.log"), new Path(homeDirectory + "/logs"));
        //遍历
        bianli(homeDirectory);
    }
    //下载文件
    @Test
    public void downloadFile() throws Exception{
        fileSystem.copyToLocalFile(false,new Path(homeDirectory+"/logs/counts"),new Path("D:/counts.txt"));
    }
    //删除目录
    @Test
    public void remove() throws Exception{
        fileSystem.deleteOnExit(new Path("hdfs://192.168.86.129:9000/user/MyPC/logs/counts.txt"));
    }

    @Test
    public void wordCount(){
    }

    @After
    public void end()throws Exception {
        homeDirectory=null;
        fileSystem.close();
    }

    public void bianli(Path path) throws IOException {
        Stack<Path> s = new Stack<Path>();
        s.push(path);
        while (!s.isEmpty()){
            Path path1 = s.pop();
            FileStatus fileLinkStatus = fileSystem.getFileLinkStatus(path1);
            boolean file = fileLinkStatus.isFile();
            if (file){
                Path path2 = fileLinkStatus.getPath();
                System.err.println(path2);
            }else{
                FileStatus[] fileStatuses1 = fileSystem.listStatus(path1);
                for (FileStatus FileStatus:fileStatuses1) {
                    s.push(FileStatus.getPath());
                }
            }
        }
    }
}
