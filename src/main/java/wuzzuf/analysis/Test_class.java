package wuzzuf.analysis;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;

@RestController
public class Test_class {


    @RequestMapping("/datav1")
    public static String datav1(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.returnDatav1(n);
    }

    @RequestMapping("/dataStructure")
    public static String dataStructure() throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.dataStructure();
    }

    @RequestMapping("/dataSummary")
    public static String dataSummary() throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.returnData_Summary();
    }

    @RequestMapping("/data")
    public static String data(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.returnData(n);
    }
    @RequestMapping("/jobs_company")
    public static String jobs_company(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.count_job_company(n);
    }

    @RequestMapping("/job_titles")
    public static String job_titles(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.most_popular_job_titles(n);
    }

    @RequestMapping("/areas")
    public static String areas(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.most_popular_areas(n);
    }

    @RequestMapping("/skills")
    public static String popularSkills(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.skills(n);
    }

    @RequestMapping("/factorization")
    public static String factorize(@RequestParam(value = "row",defaultValue = "10") int n) throws FileNotFoundException {
        JOb_DAOclass obj=new JOb_DAOclass();
        return  obj.factorizeAndConvert(n);
    }








}
