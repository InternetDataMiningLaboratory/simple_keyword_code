package module;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;

public class Keyword {
	
	public static void main(String[] args) throws Exception{
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://mysql.service.consul/company_service";
		String user = System.getenv("DATABASE_USER"); 
		String password = System.getenv("DATABASE_PASSWD");
		String keyword = null;
		ArrayList<String>  search_word = new ArrayList<String> ();
		String search_result = "";
        String key = null;
        String value = null;
		int search_id = 0;
		try{
			Class.forName(driver);
			Connection conn = DriverManager.getConnection(url, user, password);
			if(!conn.isClosed()) 
				System.out.println("Succeeded connecting to the Database!");
			String sql1 = "select * from keyword where user_id = ?";
			PreparedStatement pst1 = conn.prepareStatement(sql1);
			pst1.setString(1,args[0]);
			ResultSet rs = pst1.executeQuery();
			rs.last();  
			int rowCount = rs.getRow();
			rs.beforeFirst();
			if(rowCount == 0)
				return;
			else{
				while(rs.next()){
					keyword = rs.getString("keyword");
					search_word.add(keyword);
				}
			}
			JSONArray words = new JSONArray(search_word);
			System.out.println(words);
			String sql2 = "insert into search (search_id,search_word,search_result,search_status) values (NULL,?,NULL,NULL)";
			PreparedStatement pst2 = conn.prepareStatement(sql2);
			pst2.setString(1,words.toString());
			pst2.executeUpdate();
			String sql3 = " SELECT LAST_INSERT_ID();";
			PreparedStatement pst3 = conn.prepareStatement(sql3);
			ResultSet rs1 = pst3.executeQuery();
			rs1.next();
			search_id = rs1.getInt("last_insert_id()");
			System.out.println("search_id:"+search_id);
			
	        String sr=Keyword.sendPost("http://scheduler:9000/service/searchEngine/search", "search_id="+search_id);
	        System.out.println(sr);
			
			String sql4 = "select * from search where search_id = ?";
			PreparedStatement pst4 = conn.prepareStatement(sql4);
			pst4.setLong(1,search_id);
			//pst4.setLong(1,14);
			ResultSet rs2 = pst4.executeQuery();
			rs2.next();
			search_result = rs2.getString("search_result");
            float score_of_one_word_r = (float) (1.0/rowCount);
            float score_of_one_word = (float)(Math.round(score_of_one_word_r*10000))/10000;
            Map<Integer,Float> map = new HashMap<Integer,Float>();
			JSONObject jsonObject = new JSONObject(search_result);
			Iterator iterator = jsonObject.keys();
			while (iterator.hasNext()) {
	            key = (String) iterator.next();
	            value = jsonObject.getString(key);
	            JSONArray jsonArray = new JSONArray(value);
	            if (jsonArray.length()!=0){
	            	for (int i = 0; i < jsonArray.length(); i++){
                        if(map.containsKey(Integer.parseInt(jsonArray.getString(i))))
                            map.put(Integer.parseInt(jsonArray.getString(i)), map.get(Integer.parseInt(jsonArray.getString(i)))+score_of_one_word);
                        else
                            map.put(Integer.parseInt(jsonArray.getString(i)), score_of_one_word);
	            	}
	            }
	        }
			JSONObject result = new JSONObject(map);
			System.out.println(result);
            String sql5 = "update user set user_score = ? where user_id = ?";
            PreparedStatement pst5 = conn.prepareStatement(sql5);
            pst5.setString(1, result.toString());
            pst5.setString(2, args[0]);
            pst5.executeUpdate();

		}catch(Exception e) {
			e.printStackTrace();
		}
	}
    public static String sendPost(String url, String param) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(url);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
        //    conn.setRequestProperty("user-agent",
          //          "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(param);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(in!=null){
                    in.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }
        return result;
    }    
}
