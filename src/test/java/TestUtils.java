import com.gpjpe.helpers.Utils;
import junit.framework.TestCase;
import org.apache.hadoop.hbase.util.Bytes;


public class TestUtils extends TestCase{

    public void testKeyGen(){
        long timestamp = 1453191726000L;
        String lang = "en";
        byte[] tsPart = new byte[8];
        byte[] langPart = new byte[2];
        System.arraycopy(Utils.generateKey(timestamp, lang),0, tsPart,0,8);
        System.arraycopy(Utils.generateKey(timestamp, lang),8, langPart,0,2);

        System.out.println(Bytes.toLong(tsPart) + Bytes.toString(langPart));
        assertEquals(Bytes.toLong(tsPart),timestamp);
        assertEquals(Bytes.toString(langPart),lang);

    }
}
