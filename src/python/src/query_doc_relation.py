import pandas as pd
import numpy as np
from pathlib import Path


def group(df):
    grouped = df.groupby("query_id")["doc_id"]
    # A bit slow because sorting which is not necessary
    # grouped = grouped.apply(lambda doc_ids: ",".join([str(doc_id) for doc_id in doc_ids])).reset_index()
    grouped = grouped.apply(lambda doc_ids: ",".join([str(doc_id) for doc_id in doc_ids.sort_values()])).reset_index()
    grouped = grouped.rename(columns={"doc_id": "doc_ids"})
    return grouped


def main():
    prefix = Path("../../data/config")
    train_df = pd.read_csv(prefix / "train.marks.tsv", sep="\t", dtype="int64")
    test_df = pd.read_csv(prefix / "sample_prediction.tsv", sep="\t")
    test_df = test_df.assign(label=np.full(test_df.index.size, -1))

    query_doc_df = pd.concat([train_df, test_df], ignore_index=True)
    query_doc_df.to_csv(prefix / "samples.tsv", sep="\t", index_label="sample_id")

    grouped = group(query_doc_df)
    grouped.to_csv(prefix / "query_docs_relation.tsv", sep="\t", index=False)

    # Update with similar queries
    similarity_df = pd.read_csv(prefix / "../queries/similar_idx.tsv", sep="\t",
                                dtype={"id_a": "int64", "id_b": "int64", "score": "float"})
    upgrade_query_doc_df = query_doc_df[["query_id", "doc_id"]].merge(similarity_df[["id_a", "id_b"]],
                                                                      left_on="query_id", right_on="id_a")
    upgrade_query_doc_df = upgrade_query_doc_df[["id_b", "doc_id"]].rename(columns={"id_b": "query_id"})
    upgrade_query_doc_df = upgrade_query_doc_df.assign(label=np.full(upgrade_query_doc_df.index.size, -1))
    upgrade_query_doc_df = pd.concat([query_doc_df, upgrade_query_doc_df], ignore_index=True)
    upgrade_query_doc_df.drop_duplicates(subset=["query_id", "doc_id"], inplace=True)
    upgrade_query_doc_df.to_csv(prefix / "samples_upgraded.tsv", sep="\t", index=False)

    grouped = group(upgrade_query_doc_df)
    grouped.to_csv(prefix / "query_docs_relation_upgraded.tsv", sep="\t", index_label="sample_id")


if __name__ == "__main__":
    main()