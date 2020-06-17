import Statistics.*;
import org.apache.hadoop.fs.Path;

import java.util.Map;
import java.util.HashMap;

class Config {
    static final Path URL_MAP_FILEPATH = new Path("data/config_statistic_collect/url_id-url.tsv");
    static final Path QUERY_MAP_FILEPATH = new Path("data/config_statistic_collect/query_id-query_score.tsv");
    static final Path HOST_MAP_FILEPATH = new Path("data/config_statistic_collect/host_id-url_id.tsv");
    static final Path QUERY_DOC_CORR_MAP_FILEPATH = new Path("data/config_statistic_collect/query_id-doc_ids.tsv");

    static Map<String, BaseStatistic> StatisticMap;
    static {
        StatisticMap = new HashMap<>();

        // QUERY INDEPENDENT FACTORS
        StatisticMap.put(StatisticConfig.IMPLEMENTATION_DOC_KEY, new TImplementationDoc());
        StatisticMap.put(StatisticConfig.CLICK_DOC_KEY, new TClickDoc());
        StatisticMap.put(StatisticConfig.CTR_KEY, new TCTR());
        StatisticMap.put(StatisticConfig.AVG_POS_DIFF_LAST_DOC, new TAvgPosDiffLastDoc());
        StatisticMap.put(StatisticConfig.AVG_POS_DIFF_NEXT_DOC, new TAvgPosDiffNextDoc());

        // QUERY DEPENDENT FACTORS
        StatisticMap.put(StatisticConfig.FIRST_CTR_KEY, new TFirstCTR());
        StatisticMap.put(StatisticConfig.LAST_CTR_KEY, new TLastCTR());
        StatisticMap.put(StatisticConfig.ONLY_CTR_KEY, new TOnlyCTR());
        StatisticMap.put(StatisticConfig.PERCENT_CLICK_QUERY_DOC_KEY, new TPercentClick());

        // TIME BASED FACTORS
        StatisticMap.put(StatisticConfig.AVG_VIEW_TIME_KEY, new TAvgDocViewTime());

        // POSITION AND CLICK NUMBER BASED FACT
        StatisticMap.put(StatisticConfig.AVG_POSITION_KEY, new TAvgPosition());
        StatisticMap.put(StatisticConfig.AVG_CLICK_POSITION_KEY, new TAvgClickedPosition());
        StatisticMap.put(StatisticConfig.AVG_CLICK_NUMBER_KEY, new TAvgClickNumber());
        StatisticMap.put(StatisticConfig.AVG_INVERSE_CLICK_NUMBER_KEY, new TAvgInverseClickNumber());
        StatisticMap.put(StatisticConfig.AVG_NUMBER_CLICKED_BEFORE_KEY, new TAvgNumClickedBefore());
        StatisticMap.put(StatisticConfig.AVG_NUMBER_CLICKED_AFTER_KEY, new TAvgNumClickedAfter());

        // PROBABILISTIC BASED FACTORS
        StatisticMap.put(StatisticConfig.PROB_LAST_CLICKED_KEY, new TProbLastClicked());
        StatisticMap.put(StatisticConfig.PROB_UP_CLICK_KEY, new TProbUpClick());
        StatisticMap.put(StatisticConfig.PROB_DOWN_CLICK_KEY, new TProbDownClick());
        StatisticMap.put(StatisticConfig.PROB_DOUBLE_CLICK_KEY, new TProbDoubleClick());
        StatisticMap.put(StatisticConfig.PROB_COME_BACK_KEY, new TProbComeBack());
        StatisticMap.put(StatisticConfig.PROB_GO_BACK_KEY, new TProbGoBack());

        // QUERY AND SERP DEPEND FACTORS
        StatisticMap.put(StatisticConfig.AVG_WORK_TIME_QUERY_KEY, new TAvgWorkTimeQuery());
        StatisticMap.put(StatisticConfig.AVG_FIRST_POSITION_QUERY_KEY, new TAvgFirstPositionQuery());
        StatisticMap.put(StatisticConfig.AVG_CLICK_COUNT_QUERY_KEY, new TAvgClickCountQuery());
    }
}
