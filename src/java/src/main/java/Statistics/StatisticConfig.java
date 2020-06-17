package Statistics;


public class StatisticConfig {
    static final public String ABSENT = "-1";
    static final String EMPTY = "-2";

    // QUERY INDEPENDENT FACTORS
    public static final String IMPLEMENTATION_DOC_KEY = "0";
    public static final String CLICK_DOC_KEY = "1";
    public static final String CTR_KEY = "2";
    public static final String AVG_POS_DIFF_LAST_DOC = "3";
    public static final String AVG_POS_DIFF_NEXT_DOC = "4";

    // QUERY DEPENDENT FACTORS
    public static final String FIRST_CTR_KEY = "5";
    public static final String LAST_CTR_KEY = "6";
    public static final String ONLY_CTR_KEY = "7";
    public static final String PERCENT_CLICK_QUERY_DOC_KEY = "8";

    // TIME BASED FACTORS
    public static final String AVG_VIEW_TIME_KEY = "9";

    // POSITION AND CLICK NUMBER BASED FACTOR
    public static final String AVG_POSITION_KEY = "10";
    public static final String AVG_CLICK_POSITION_KEY = "11";
    public static final String AVG_CLICK_NUMBER_KEY = "12";
    public static final String AVG_INVERSE_CLICK_NUMBER_KEY = "13";
    public static final String AVG_NUMBER_CLICKED_BEFORE_KEY = "14";
    public static final String AVG_NUMBER_CLICKED_AFTER_KEY = "15";

    // PROBABILISTIC BASED FACTORS
    public static final String PROB_LAST_CLICKED_KEY = "16";
    public static final String PROB_UP_CLICK_KEY = "17";
    public static final String PROB_DOWN_CLICK_KEY = "18";
    public static final String PROB_DOUBLE_CLICK_KEY = "19";
    public static final String PROB_COME_BACK_KEY = "20";
    public static final String PROB_GO_BACK_KEY = "21";

    // QUERY AND SERP DEPEND FACTORS
    public static final String AVG_WORK_TIME_QUERY_KEY = "22";
    public static final String AVG_FIRST_POSITION_QUERY_KEY = "23";
    public static final String AVG_CLICK_COUNT_QUERY_KEY = "24";

    public static final Integer KEY_CNT = 25;
}
