package wuzzuf.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.style.Styler;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

public class BarCharts implements ExampleChart<CategoryChart> {
    // Method for creating bar chart of top ten Titles in dataset:
    public static void popularTitles(Dataset<Row> topTenTitles){
        ExampleChart<CategoryChart> exampleChart = new BarCharts();
        // converting columns into lists for plotting pie chart
        List<Row> top_titles = topTenTitles.select("Title").collectAsList().stream().collect(Collectors.toList());
        List<Row> counts = topTenTitles.select("TitlesCount").collectAsList().stream().collect(Collectors.toList());
        List<String> topTitles = new ArrayList<>();
        for (Row x : top_titles){
            topTitles.add(x.get(0).toString());
        }
        List<Double> jobs_per_company = new ArrayList<>();
        for (Row x : counts){
            jobs_per_company.add(Double.parseDouble(x.get(0).toString()));
        }
        String xaxis = "Title_name";
        String title = "Top Titles";
        CategoryChart chart = exampleChart.getChart(topTitles, jobs_per_company, title, xaxis);
        //new SwingWrapper<>(chart).displayChart();
        try {
            BitmapEncoder.saveBitmap(chart, System.getProperty("user.dir")+"/Public/topTitles", BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }


    // Method for creating bar chart of top ten Titles in dataset:
    public static void popularAreas(Dataset<Row> topTenAreas) {
        ExampleChart<CategoryChart> exampleChart = new BarCharts();

        // converting columns into lists for plotting pie chart
        List<Row> top_Areas = topTenAreas.select("Location").collectAsList().stream().collect(Collectors.toList());
        List<Row> counts = topTenAreas.select("Location_count").collectAsList().stream().collect(Collectors.toList());

        List<String> topAreas = new ArrayList<>();
        for (Row x : top_Areas){
            topAreas.add(x.get(0).toString());
        }

        List<Double> Counts = new ArrayList<>();
        for (Row x : counts){
            Counts.add(Double.parseDouble(x.get(0).toString()));
        }
//        E:\Wuzzuf_Jobs_DataAnalysis-main\Wuzzuf\Public
//        E:\Wuzzuf_Jobs_DataAnalysis-main\Wuzzuf\src\main\resources
        String xaxis = "Areas";
        String title = "Top Areas";
        CategoryChart chart = exampleChart.getChart(topAreas, Counts, title, xaxis);
        //new SwingWrapper<>(chart).displayChart();
        try {
            BitmapEncoder.saveBitmap(chart, System.getProperty("user.dir")+"/Public/topAreas",BitmapEncoder.BitmapFormat.PNG);
        }
        catch (Exception e){
            System.out.println(e);
        }
    }


    @Override
    public CategoryChart getChart(List<String> keys, List<Double> values, String title, String xaxis) {

        // Create Chart
        CategoryChart chart =
                new CategoryChartBuilder()
                        .width(2000)
                        .height(1000)
                        .title(title)
                        .xAxisTitle(xaxis)
                        .yAxisTitle("Counts")
                        .build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        //chart.getStyler().setLabelsVisible(false);
        chart.getStyler().setPlotGridLinesVisible(false);

        // Series
        chart.addSeries(xaxis, keys, values);

        return chart;
    }

}
