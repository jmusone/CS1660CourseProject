import java.io.*; 
import java.util.*; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.mapreduce.Mapper; 
  
public class topNWordsMapper extends Mapper<Object, Text, Text, LongWritable> { 
  
    private TreeMap<Long, String> tmap; 
  
    @Override
    public void setup(Context context) throws IOException, InterruptedException { 
        tmap = new TreeMap<Long, String>(); 
    } 
  
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException { 

        String[] tokens = value.toString().split("\t"); 
        Configuration conf = context.getConfiguration();
        int topN = conf.getInt("nValue", Integer.MIN_VALUE);
        String name = key.toString();   
        String word = tokens[0]; 
        long total = getTotal(tokens[1]);

        tmap.put(total, word); 

        if (tmap.size() > 10) { 
            tmap.remove(tmap.firstKey()); 
        } 
    } 

    private long getTotal(String hashMapOutput){
        boolean inValue = false;
        long totalVal = 0;
        long tempVal = 0;
        
        for(int i = 0; i < hashMapOutput.length(); i++){
            if(inValue){
                if(isDigit(hashMapOutput.charAt(i))){
                    tempVal = tempVal * 10;
                    tempVal += Integer.parseInt(String.valueOf(hashMapOutput.charAt(i))); 
                }else{
                    inValue = false;
                    totalVal += tempVal;
                    tempVal = 0;
                }
            }

            if(hashMapOutput.charAt(i) == '='){
                inValue = true;
            }
        }
        return totalVal;
    }
    
    private boolean isDigit(char c) {
        return (c == '0' || c == '1' || c == '2' || c == '3' || c == '4' || c == '5' || c == '6' || c == '7' || c == '8' || c == '9');
    }
  
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException { 
        for (Map.Entry<Long, String> entry : tmap.entrySet())  { 
  
            long count = entry.getKey(); 
            String name = entry.getValue(); 
  
            context.write(new Text(name), new LongWritable(count)); 
        } 
    } 
} 