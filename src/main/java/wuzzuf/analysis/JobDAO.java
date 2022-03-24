package wuzzuf.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.HashMap;
import java.util.LinkedHashMap;

public interface JobDAO {
public void read_data();
public void clean_data();
public String count_job_company(int limit);
public String most_popular_job_titles(int limit);
public String most_popular_areas(int limit);
public String skills(int limit);
public String returnDatav1(int limit);
public String returnData(int limit);
public  String dataStructure();
public String returnData_Summary();
public String factorizeAndConvert(int limit);
}
