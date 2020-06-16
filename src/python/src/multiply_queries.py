import regex as re
from random import randint, random
from tqdm import tqdm
import pandas as pd


queries = pd.read_csv("../data/queries/queries_spell_corrected.tsv", sep="\t", index_col=0)
similarity_df = pd.read_csv("../data/queries/similar_words.tsv", sep="\t")
similarity_df["word_a"] = similarity_df["word_a"].apply(str.lower)
similarity_df["word_b"] = similarity_df["word_b"].apply(str.lower).apply(lambda x: re.sub(r'[^\w\s]', '', x))
similarity_df.drop_duplicates(subset=["word_a", "word_b"], inplace=True)
similarity_df = similarity_df[similarity_df["word_a"] != similarity_df["word_b"]]
similarity_df = similarity_df[similarity_df["word_a"].str.isalpha()]

similar_groups = similarity_df.groupby("word_a")


with open("../data/queries/wider_queries.tsv", "w") as output_stream:
    valid_set = set(similarity_df["word_a"].unique())
    for key, row in tqdm(list(queries.iterrows())):
        words = row["text"].split()
        new_queries = set()
        new_queries.add(tuple([row["text"],  1]))
        for i in range(20):
            new_query = []
            score = 1
            for word in words:
                if word in valid_set:
                    sim_words = similar_groups.get_group(word)
                    idx = randint(0, sim_words.index.size - 1)
                    sim_word = sim_words["word_b"].values[idx]
                    if random() < 0.3:
                        score *= sim_words["similarity"].values[idx]
                        new_query.append(sim_word)
                    else:
                        new_query.append(word)
                else:
                    new_query.append(word)
            new_queries.add(tuple([" ".join(new_query), score]))
        # print(new_queries)
        for item in new_queries:
            output_stream.write("{}\t{}\t{:04.2f}\n".format(key, item[0], item[1]))

    raw_queries = pd.read_csv("../data/queries/queries.tsv", sep="\t", index_col=0)
    for key, row in raw_queries.iterrows():
        output_stream.write("{}\t{}\t1.00\n".format(key, row["text"]))

df = pd.read_csv("../data/queries/wider_queries.tsv", sep="\t", index_col=0, names=["text", "score"])
df = df.drop_duplicates(subset=["text"]).sort_index()
df.to_csv("../data/queries/wider_queries.tsv", sep="\t", index_label="query_id")
