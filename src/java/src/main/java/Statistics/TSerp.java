package Statistics;

import javafx.util.Pair;

import java.util.List;

public class TSerp {

    static class TBaseLink {
        String UrlID;
        String HostID;
        boolean QueryCorrespondent;

        TBaseLink() {
            UrlID = StatisticConfig.EMPTY;
            HostID = StatisticConfig.EMPTY;
            QueryCorrespondent = false;
        }

        public boolean getCorrespondence() {
            return QueryCorrespondent;
        }
    }

    public static class TShownLinks extends TBaseLink {

        public TShownLinks(String urlID, String hostID, boolean queryCorrespondent) {
            UrlID = urlID;
            HostID = hostID;
            QueryCorrespondent = queryCorrespondent;
        }
    }

    public static class TClickedLink extends TBaseLink{

        public Long Timestamp;
        Integer Position;

        public TClickedLink(TShownLinks other) {
            UrlID = other.UrlID;
            HostID = other.HostID;
            QueryCorrespondent = other.QueryCorrespondent;
            Timestamp = 0L;
            Position = 0;
        }

        public void setTimestamp(Long timestamp) {
            Timestamp = timestamp;
        }

        public void setPosition(Integer position) {
            Position = position;
        }
    }

    String QueryID;
    String RegionID;
    Float QuerySimilarity;
    List<TShownLinks> ShownLinks;
    List<TClickedLink> ClickedLinks;

    public TSerp(Pair<String, Float> query, String regionID, List<TShownLinks> shownLinks, List<TClickedLink> clickedLinks) {
        QueryID = query.getKey();
        QuerySimilarity = query.getValue();
        RegionID = regionID;
        ShownLinks = shownLinks;
        ClickedLinks = clickedLinks;
    }

    public int shownLinkIndex(String urlID) {
        int pos = -1;
        for (int i = 0; i < ShownLinks.size(); ++i) {
            if (ShownLinks.get(i).UrlID.equals(urlID)) {
                return i;
            }
        }
        return pos;
    }
}