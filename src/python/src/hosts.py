from pathlib import Path
import re
import pandas as pd
reg = re.compile(r"(^(https?\.?|www\.?|://){1,2}|/$)")


def extractHost(url):
    norm_url = reg.sub("", url)
    pos = norm_url.find("/")
    if pos < 0:
        pos = len(norm_url)
    return norm_url[:pos]


def main():
    prefix = Path("../../data/config")
    urls_df = pd.read_csv(prefix / "url.tsv", sep="\t")
    urls_df = urls_df.assign(host=urls_df["url"].apply(extractHost))

    hosts = pd.DataFrame(urls_df["host"].unique(), columns=["host", ])
    hosts = hosts.assign(host_id=hosts.index)
    urls_df = urls_df.merge(hosts, left_on="host", right_on="host")
    urls_df[["host_id", "url_id"]].to_csv(prefix / "host_url_relation.tsv", index=False, sep="\t")

    relation = urls_df.groupby("host_id")["url_id"].apply(lambda x: ",".join([str(y) for y in x])).reset_index()
    relation = relation.rename(columns={"url_id": "url_ids"})
    relation.to_csv(prefix / "host_urls_relation.tsv", sep="\t", index=False)


if __name__ == "__main__":
    main()
