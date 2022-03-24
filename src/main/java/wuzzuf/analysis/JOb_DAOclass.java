package wuzzuf.analysis;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;



public class JOb_DAOclass implements JobDAO{

    private SparkSession sparkSession=null;
    private Dataset<Row> data = null;
    private  Dataset<Row> datav1 = null;
    private String path = "src/main/resources/Wuzzuf_Jobs.csv";

    public JOb_DAOclass() {
        this.read_data();
        this.clean_data();
    }

    public Dataset<Row> getData() {
        return data;
    }

    public void setData(Dataset<Row> data) {
        this.data = data;
    }


    @Override
    public void read_data() {
         this.sparkSession = SparkSession.builder ().appName ("Wuzzuf Application ").master ("local[5]")
                .getOrCreate ();
        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = this.sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
        this.datav1 = dataFrameReader.csv (path);

    }


    @Override
    public void clean_data() {
        this.data = this.datav1.withColumn ("YearsExp",datav1.col ("YearsExp").cast("String"))
                .filter (datav1.col("YearsExp").notEqual("null Yrs of Exp"));
        this.data=this.data.distinct();
    }

    @Override
    public String returnDatav1(int number) {
        this.datav1.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        Dataset<Row> head =this.sparkSession.sql("SELECT * FROM Wuzzuf_Jobs_table LIMIT "+ number+";");
        return getOutput(head, "Read Data Set");
    }

    @Override
    public  String dataStructure(){
        String structure = this.datav1.schema().toString();
        String str = structure.replace("StructType", "").replace("StructField","").replace("(","")
                .replace(")","").replace("StringType","String");
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid ForestGreen ;\n" +
                "}\n" +
                "</style> <body>";
        output += "<h1>"+"Job Data Structure"+"</h1> <table style=\"width:100%\">"
                +"<tr>\n" +
                "    <th>Column Name </th>\n" +
                "    <th>Column Type</th>\n" +
                "  </tr>";
        String[] output_list = str.split(",");
        //String[] x = {"Column Name", "Column Type", "Nullable"};
        for(int i = 0 ; i < output_list.length ;){
            output+="<tr>";
            int j = i;
            for( j = i ; j < i+3 ; j++){
                if (output_list[j].equals("true")){
                    continue;
                }
                output +=  "<td>"+" "+output_list[j]+"</td>";
            }
            output += "</tr> ";
            i = j;
        }

        output += "</table>"+"</body></html>";
        return output;
    }
@Override
    public String returnData_Summary() {
        this.datav1.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        Dataset<Row> head =this.sparkSession.sql("SELECT  Count( DISTINCT Title) as Title , Count( DISTINCT Company) as Company ,Count (DISTINCT Location ) as Location ,Count (DISTINCT  Type) as Type ,Count (DISTINCT  Level) as Level ,Count (DISTINCT  YearsExp) as YearsExp ,Count (DISTINCT  Country) as Country ,Count (DISTINCT  Skills) as Skills  FROM Wuzzuf_Jobs_table ;");
        head.show();
        return getOutput(head, "Data structure");
    }



    @Override
    public String returnData(int limit) {
        this.data.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        Dataset<Row> head =this.sparkSession.sql("SELECT * FROM Wuzzuf_Jobs_table LIMIT "+ limit+";");
        return getOutput(head, "Read Data Set After Clean ");
    }


    @Override
    public String count_job_company(int limit) {
        this.data.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        String job_company = "SELECT Company, COUNT(Title) AS JobCount " +
                "FROM Wuzzuf_Jobs_table " +
                "GROUP BY Company " +
                "ORDER BY JobCount DESC "+
                "LIMIT " +limit+
                " ";
         Dataset<Row> jobs_company = sparkSession.sql(job_company );
         PieCharts.popularCompanies(jobs_company);
        return getOutput_count_job_company(jobs_company, "Count the jobs for each company and display that in order");
    }

    @Override
    public String most_popular_job_titles( int limit) {
        data.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        String most_popular_job  = "SELECT Title, COUNT(*) AS TitlesCount  " +
                "FROM Wuzzuf_Jobs_table " +
                "GROUP BY Title " +
                "ORDER BY TitlesCount DESC "+
                "LIMIT "+limit+" ";
        Dataset<Row> most_popular_titles  = sparkSession.sql(most_popular_job );
        BarCharts.popularTitles(most_popular_titles);
        return getOutput_job_title(most_popular_titles,"Find out what are it the most popular job titles?");
    }

    @Override
    public String most_popular_areas(int limit) {
        data.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        String most_popular_area="SELECT Location, COUNT(*) AS Location_count " +
                "FROM Wuzzuf_Jobs_table " +
                "GROUP BY Location " +
                "ORDER BY Location_count DESC "+
                "LIMIT "+limit+" ";
        Dataset<Row> most_popular_areas  = sparkSession.sql(most_popular_area );
        BarCharts.popularAreas(most_popular_areas);
        return getOutput_areas(most_popular_areas,"Find out the most popular areas?");
    }


    @Override
    public String skills(int limit) {
        data.createOrReplaceTempView ("Wuzzuf_Jobs_table");
        String skill="SELECT Skills FROM Wuzzuf_Jobs_table";
        final Dataset<Row> skills= sparkSession.sql(skill);
        List<Row> skillsr = skills.collectAsList().stream().collect(Collectors.toList());
        HashMap<String,Integer> topskills = new HashMap<>();
        for (Row x : skillsr){
            String[]skill_string=x.toString().split(",");
            for (String s:skill_string){
                s=s.trim().trim().replace("[","").replace("]","");
                if (topskills.containsKey(s)){
                    topskills.put(s,topskills.get(s)+1);
                }
                else {
                    topskills.put(s,1);
                }
            }
        }
        topskills = sortByValue(topskills);
        // Take top ten Skills
        LinkedHashMap<String,Integer> topTenSkills = new LinkedHashMap<>();
        // create iterator to iterate over the linkedHashMap
        Iterator<Map.Entry<String, Integer>> skillFreqIterator = topskills.entrySet().iterator();
        for (int i = 0; i<limit ; i++) {
            Map.Entry<String, Integer> entry = skillFreqIterator.next();
            topTenSkills.put(entry.getKey() , entry.getValue());
        }

        //output:
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid DodgerBlue;\n" +
                "}\n" +
                "</style> <body>";
        output += "<h1>Find out the most important skills required?</h1><table style=\"width:100%\">"
                +"<tr>\n" +
                "    <th>Skill</th>\n" +
                "    <th>Counts</th>\n" +
                "  </tr>";

        for(String k : topTenSkills.keySet()){
            output += "<tr> ";
            output += "<td>"+k+"</td>";
            output += "<td>"+topTenSkills.get(k)+"</td>";
            output += "</tr> ";

        }
        output+="</table>";
        output += "</body></html>";
        return output;


    }


@Override
    public String factorizeAndConvert(int limit){

        StringIndexerModel indexer = new StringIndexer()
                .setInputCol("YearsExp")
                .setOutputCol("YearsExp_Encoding")
                .fit(this.data);

        Dataset<Row> factorize = indexer.transform(this.data);
        factorize .createOrReplaceTempView ("Wuzzuf_Jobs_table");
        Dataset<Row> head =this.sparkSession.sql("SELECT * FROM Wuzzuf_Jobs_table LIMIT "+ limit+";");
        return getOutputfactorize(head, "Data After Factorization");
    }
    private static LinkedHashMap<String, Integer> sortByValue(HashMap<String, Integer> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<String, Integer> > list =
                new LinkedList<Map.Entry<String, Integer> >(hm.entrySet());
        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<String, Integer> >() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
        // put data from sorted list to hashmap
        LinkedHashMap<String, Integer> temp = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    public String getOutput(Dataset<Row> ds, String header) {
        List<String> data = ds.toJSON().collectAsList();
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid MediumAquamarine;\n" +
                "}\n" +
                "</style> <body>";
        output += "<h1>"+header+"</h1> <table style=\"width:100%\">"
        +"<tr>\n" +
                "    <th>Title</th>\n" +
                "    <th>Company</th>\n" +
                "    <th>Location</th>\n" +
                "    <th>Type</th>\n" +
                "    <th>Level</th>\n" +
                "    <th>YearsExp</th>\n" +
                "    <th>Country</th>\n" +
                "    <th>Skills</th>\n" +
                "  </tr>";
        for (String str : data){
            String s = str.toString().replace("\"", "").replace("{","").replace("}","")
                    .replace("Title","").replace("Company","").replace("Location","").replace("Type","")
                    .replace("Level","").replace("YearsExp","").replace("Country","").replace("Skills","").replace(":","");
            String[] strList = s.split(",",8);
            output+="<tr>";
            for (String attrib : strList){
                output += "<td>"+attrib+"</td>";
            }
            output += "</tr> ";
        }
        output += "</table>"+"</body></html>";
        return output;
    }

    public String getOutput_count_job_company(Dataset<Row> ds, String header) {
        List<String> data = ds.toJSON().collectAsList();
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid brown;\n" +
                "}\n" +
                "</style> <body>";
        output += "<h1>"+header+"</h1> <table style=\"width:100%\">"
                +"<tr>\n" +
                "    <th>Company Name</th>\n" +
                "    <th>Count of Jobs</th>\n" +
                "  </tr>";

        for (String str : data){
            String s = str.toString().replace("\"", "").replace("{","").replace("}","")
                    .replace(":","")
                    .replace("Company","").replace("JobCount","");
            String[] strList = s.split(",",2);
            output+="<tr>";
            for (String attrib : strList){
                output += "<td>"+attrib+"</td>";
            }
            output += "</tr> ";
        }
        output+="</table>";
        String s="topCompanies.png";
        output+="<center><img src="+s+" alt=\"Top Companies\" style=\"vertical-align:middle;margin:50px\"></center>";
        output += "</body></html>";
        return output;
    }

    public String getOutput_job_title(Dataset<Row> ds, String header) {
        List<String> data = ds.toJSON().collectAsList();
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid brown;\n" +
                "}\n" +
                "</style> <body>";
        //output += "<h1>"+header+"</h1><br><p>";
        output += "<h1>"+header+"</h1> <table style=\"width:100%\">"
                +"<tr>\n" +
                "    <th>Job_Title</th>\n" +
                "    <th>Count</th>\n" +
                "  </tr>";

        for (String str : data){
            String s = str.toString().replace("\"", "").replace("{","").replace("}","")
                    .replace(":","")
                    .replace("Title","").replace("sCount","");
            String[] strList = s.split(",",8);

            output+="<tr>";
            for (String attrib : strList){
                output += "<td>"+attrib+"</td>";
            }
            output += "</tr> ";
        }
        output+="</table>";
        String s="topTitles.png";
        output+="<center><img src="+s+" alt=\"Top Companies\" style=\"object-fit:fill ;width:1500px;height:800px;margin:50px\"></center>";
        output += "</body></html>";
        return output;
    }

    public String getOutput_areas(Dataset<Row> ds, String header) {
        List<String> data = ds.toJSON().collectAsList();
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid brown;\n" +
                "}\n" +
                "</style> <body>";
        output += "<h1>"+header+"</h1> <table style=\"width:100%\">"
                +"<tr>\n" +
                "    <th>Area</th>\n" +
                "    <th>Count</th>\n" +
                "  </tr>";
        for (String str : data){
            String s = str.toString().replace("\"", "").replace("{","").replace("}","")
                    .replace(":","")
                    .replace("Location","").replace("_count","");

            String[] strList = s.split(",",8);

            output+="<tr>";
            for (String attrib : strList){
                output += "<td>"+attrib+"</td>";
            }
            output += "</tr> ";
        }
        output+="</table>";
        String s="topAreas.png";
        output+="<center><img src="+s+" alt=\"Top Companies\" style=\"vertical-align:middle;margin:50px\"></center>";
        output += "</body></html>";
        return output;
    }
    public String getOutputfactorize(Dataset<Row> ds, String header) {
        Dataset<Row> ds2=ds.drop("Skills");
        List<String> data = ds2.toJSON().collectAsList();
        String output = "<html> <style> h1{color:#483D8B;text-align: center; margin:50px }\n" +
                "table, th,td {\n" +
                "  border:2px solid Violet;\n" +
                "}\n" +
                "</style> <body>";
        output += "<h1>"+header+"</h1> <table style=\"width:100%\">"
                +"<tr>\n" +
                "    <th>Title</th>\n" +
                "    <th>Company</th>\n" +
                "    <th>Location</th>\n" +
                "    <th>Type</th>\n" +
                "    <th>Level</th>\n" +
                "    <th>YearsExp</th>\n" +
                "    <th>Country</th>\n" +
                "    <th>YearsExp_Encoding</th>\n" +

                "  </tr>";
        for (String str : data){
            String s = str.toString().replace("\"", "").replace("{","").replace("}","")
                    .replace("Title","").replace("Company","").replace("Location","").replace("Type","")
                    .replace("Level","").replace("YearsExp","").replace("Country","")
                    .replace("Skills","").replace(":","").replace("YearsExp_Encoding","")
                    .replace("_Encoding","");

            String[] strList = s.split(",",8);
            output+="<tr>";
            for (String attrib : strList){
                output += "<td>"+attrib+"</td>";
            }
            output += "</tr> ";
        }
        output += "</table>"+"</body></html>";
        return output;
    }


}
