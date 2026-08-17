package com.atalaia.correlation.beans;

import org.primefaces.model.charts.ChartData;
import org.primefaces.model.charts.axes.cartesian.CartesianScales;
import org.primefaces.model.charts.axes.cartesian.linear.CartesianLinearAxes;
import org.primefaces.model.charts.bar.BarChartDataSet;
import org.primefaces.model.charts.bar.BarChartModel;
import org.primefaces.model.charts.bar.BarChartOptions;
import org.primefaces.model.charts.optionconfig.legend.Legend;
import org.primefaces.model.charts.optionconfig.title.Title;

import javax.annotation.PostConstruct;
import javax.inject.Named;
import javax.faces.view.ViewScoped;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Named("statisticsBean")
@ViewScoped
public class StatisticsBean implements Serializable {

    private BarChartModel hsChart;
    private BarChartModel chngChart;
    private BarChartModel repeticionChart;
    private BarChartModel delayChart;
    private BarChartModel velocidadLtChart;
    private BarChartModel velocidadStChart;
    private BarChartModel prob13Chart;
    private BarChartModel prob47Chart;
    private BarChartModel comportamientoChart;
    private BarChartModel conductaChart;
    private BarChartModel prob24AbreCierraChart;
    private BarChartModel prob57AbreCierraChart;

    @PostConstruct
    public void init() {
        createCharts();
    }

    private void createCharts() {
        hsChart = createBarModel("HS", "Horas", new Number[]{65, 59, 80, 81, 56, 55, 40});
        chngChart = createBarModel("CHNG", "Cambio", new Number[]{12, 19, 3, 5, 2, 3, 10});
        repeticionChart = createBarModel("Días que se repite la conducta", "Días", new Number[]{1, 2, 3, 2, 1, 4, 1});
        delayChart = createBarModel("Delay de la conducta", "Delay", new Number[]{0.5, 1.2, 0.8, 1.5, 2.0, 0.4, 0.9});
        velocidadLtChart = createBarModel("Velocidad LT", "Velocidad", new Number[]{100, 150, 130, 170, 160, 180, 190});
        velocidadStChart = createBarModel("Velocidad ST", "Velocidad", new Number[]{20, 30, 25, 35, 40, 45, 50});
        prob13Chart = createBarModel("PROBABILIDAD 1-3 PASOS", "Probabilidad (%)", new Number[]{10, 30, 60});
        prob47Chart = createBarModel("PROBABILIDAD 4-7 PASOS", "Probabilidad (%)", new Number[]{15, 25, 40, 20});
        comportamientoChart = createBarModel("Después de un... viene:", "Frecuencia", new Number[]{40, 30, 20, 10});
        conductaChart = createBarModel("Conducta", "Frecuencia", new Number[]{50, 25, 15, 10});
        prob24AbreCierraChart = createBarModel("PROBABILIDAD 2-4 ABRE/CIERRA", "Probabilidad (%)", new Number[]{45, 35, 20});
        prob57AbreCierraChart = createBarModel("PROBABILIDAD 5-7 ABRE/CIERRA", "Probabilidad (%)", new Number[]{30, 50, 20});
    }

    private BarChartModel createBarModel(String titleText, String label, Number[] values) {
        BarChartModel model = new BarChartModel();
        ChartData data = new ChartData();

        BarChartDataSet barDataSet = new BarChartDataSet();
        barDataSet.setLabel(label);

        List<Number> dataList = new ArrayList<>();
        for(Number n : values) {
            dataList.add(n);
        }
        barDataSet.setData(dataList);

        List<String> bgColor = new ArrayList<>();
        bgColor.add("rgba(35, 154, 85, 0.6)");
        bgColor.add("rgba(204, 164, 112, 0.6)");
        bgColor.add("rgba(255, 77, 77, 0.6)");
        bgColor.add("rgba(54, 162, 235, 0.6)");
        bgColor.add("rgba(153, 102, 255, 0.6)");
        bgColor.add("rgba(255, 159, 64, 0.6)");
        bgColor.add("rgba(255, 205, 86, 0.6)");
        barDataSet.setBackgroundColor(bgColor);

        List<String> borderColor = new ArrayList<>();
        borderColor.add("rgb(35, 154, 85)");
        borderColor.add("rgb(204, 164, 112)");
        borderColor.add("rgb(255, 77, 77)");
        borderColor.add("rgb(54, 162, 235)");
        borderColor.add("rgb(153, 102, 255)");
        borderColor.add("rgb(255, 159, 64)");
        borderColor.add("rgb(255, 205, 86)");
        barDataSet.setBorderColor(borderColor);
        barDataSet.setBorderWidth(1);

        data.addChartDataSet(barDataSet);

        List<String> labels = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            labels.add("Cat " + (i + 1));
        }
        data.setLabels(labels);
        model.setData(data);

        BarChartOptions options = new BarChartOptions();
        CartesianScales cScales = new CartesianScales();
        CartesianLinearAxes linearAxes = new CartesianLinearAxes();
        linearAxes.setOffset(true);
        cScales.addYAxesData(linearAxes);
        options.setScales(cScales);

        Title title = new Title();
        title.setDisplay(true);
        title.setText(titleText);
        options.setTitle(title);

        Legend legend = new Legend();
        legend.setDisplay(true);
        legend.setPosition("top");
        options.setLegend(legend);

        model.setOptions(options);
        return model;
    }

    private static BarChartModel createEmptyBarChartModel() {
        BarChartModel model = new BarChartModel();
        ChartData data = new ChartData();
        model.setData(data);
        return model;
    }

    public BarChartModel getHsChart() { return hsChart != null ? hsChart : createEmptyBarChartModel(); }
    public BarChartModel getChngChart() { return chngChart != null ? chngChart : createEmptyBarChartModel(); }
    public BarChartModel getRepeticionChart() { return repeticionChart != null ? repeticionChart : createEmptyBarChartModel(); }
    public BarChartModel getDelayChart() { return delayChart != null ? delayChart : createEmptyBarChartModel(); }
    public BarChartModel getVelocidadLtChart() { return velocidadLtChart != null ? velocidadLtChart : createEmptyBarChartModel(); }
    public BarChartModel getVelocidadStChart() { return velocidadStChart != null ? velocidadStChart : createEmptyBarChartModel(); }
    public BarChartModel getProb13Chart() { return prob13Chart != null ? prob13Chart : createEmptyBarChartModel(); }
    public BarChartModel getProb47Chart() { return prob47Chart != null ? prob47Chart : createEmptyBarChartModel(); }
    public BarChartModel getComportamientoChart() { return comportamientoChart != null ? comportamientoChart : createEmptyBarChartModel(); }
    public BarChartModel getConductaChart() { return conductaChart != null ? conductaChart : createEmptyBarChartModel(); }
    public BarChartModel getProb24AbreCierraChart() { return prob24AbreCierraChart != null ? prob24AbreCierraChart : createEmptyBarChartModel(); }
    public BarChartModel getProb57AbreCierraChart() { return prob57AbreCierraChart != null ? prob57AbreCierraChart : createEmptyBarChartModel(); }
}
