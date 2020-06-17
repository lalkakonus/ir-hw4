import numpy as np
from pathlib import Path
import re
import pandas as pd
reg = re.compile(r"(^(https?\.?|www\.?|://){1,2}|/$)")
prefix = Path("data/config")


def DCG(y):
    return (np.power(2, y) / np.log2(np.arange(2, y.size + 2))).sum()


def NDCG(y, k=5):
    max_dcg = DCG(np.sort(y)[::-1][:k])
    if max_dcg == 0:
        return 0
    return DCG(y[:k]) / max_dcg


def _extract_host(url):
    norm_url = reg.sub("", url)
    pos = norm_url.find("/")
    if pos < 0:
        pos = len(norm_url)
    return norm_url[:pos]


def host_relation_maker():
    urls_df = pd.read_csv(prefix / "url.tsv", sep="\t")
    urls_df = urls_df.assign(host=urls_df["url"].apply(_extract_host))

    # host_id -> url_id relation
    host_url_relation = pd.DataFrame(urls_df["host"].unique(), columns=["host", ])
    host_url_relation = host_url_relation.assign(host_id=host_url_relation.index)
    host_url_relation = urls_df.merge(host_url_relation, left_on="host", right_on="host")
    host_url_relation[["host_id", "url_id"]].to_csv(prefix / "host_url_relation.tsv", index=False, sep="\t")

    # host_id -> {url_id} relation
    host_urls_relation = urls_df.groupby("host_id")
    host_urls_relation = host_urls_relation["url_id"].apply(lambda x: ",".join([str(y) for y in x])).reset_index()
    host_urls_relation = host_urls_relation.rename(columns={"url_id": "url_ids"})
    host_urls_relation.to_csv(prefix / "host_urls_relation.tsv", sep="\t", index=False)


def samples_relation_maker():
    train_df = pd.read_csv(prefix / "train.marks.tsv", sep="\t", dtype="int64")
    test_df = pd.read_csv(prefix / "sample_prediction.tsv", sep="\t")
    test_df = test_df.assign(label=np.full(test_df.index.size, -1))

    query_doc_df = pd.concat([train_df, test_df], ignore_index=True)
    query_doc_df.to_csv(prefix / "samples.tsv", sep="\t", index_label="sample_id")

    grouped = query_doc_df.groupby("query_id")["doc_id"]
    # A bit slow because sorting which is not necessary
    # grouped = grouped.apply(lambda doc_ids: ",".join([str(doc_id) for doc_id in doc_ids])).reset_index()
    grouped = grouped.apply(lambda doc_ids: ",".join([str(doc_id) for doc_id in doc_ids.sort_values()])).reset_index()
    grouped = grouped.rename(columns={"doc_id": "doc_ids"})
    grouped.to_csv(prefix / "query_docs_relation.tsv", sep="\t", index=False)


def main():
    host_relation_maker()
    samples_relation_maker()


if __name__ == "__main__":
    main()