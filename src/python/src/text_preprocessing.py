import requests
import pandas as pd
from pathlib import Path
from fast_text import FastText
from progress.bar import Bar
from pymystem3 import Mystem
import xml.etree.ElementTree as Etree
from nltk.corpus import stopwords as nltk_stopwords
from sklearn.feature_extraction.text import CountVectorizer
from time import time


class Cutter:

    def __init__(self, limit=100):
        self.limit = limit
        self.path_postfix = "_cutted.tsv"

    def __str__(self):
        return "Cutter"

    def transform(self, data, output_path):
        data["text"] = data["text"].apply(lambda x: " ".join(x.split()[:self.limit]))
        data.to_csv(output_path, sep="\t")
        return data


class Lower:

    def __init__(self):
        self.path_postfix = "_lowered.tsv"

    def __str__(self):
        return "Lower"

    @staticmethod
    def transform(data, output_path):
        data["text"] = data["text"].apply(str.lower)
        data.to_csv(output_path, sep="\t")
        return data


class Speller:

    def __init__(self, batch_size=20):
        self.url_prefix = "https://speller.yandex.net/services/spellservice/checkTexts?"
        self.batch_size = batch_size
        self.OK_RESPONSE_CODE = 200
        self.path_postfix = "_spell_corrected.tsv"

    def __str__(self):
        return "Speller"

    def transform(self, data: pd.DataFrame, output_path):
        with open(output_path, "w") as output_file:
            output_file.write("id\ttext\n")
            for i in Bar("Spelling progress (batches)", max=data.index.size,
                         suffix='%(percent).1f%% - %(eta)ds').iter(range(0, data.index.size,
                                                                                        self.batch_size)):
                corrected_batch, code = self._send_request(data[i: i + self.batch_size])
                if code != "OK":
                    print("Data samples [{}, {}) wasn't processed".format(i, i + self.batch_size))
                    continue
                for j in range(i, i + corrected_batch.index.size):
                    output_file.write("{}\t{}\n".format(j, data.loc[j, "text"]))
        return data

    def _send_request(self, data):
        params = []
        for key, line in data.iterrows():
            params.append("text=" + "+".join(line.text.split()))
        params = "&".join(params)
        try:
            response = requests.get(self.url_prefix + params)
        except Exception as err:
            print(err)
            return pd.DataFrame()
        if response.status_code != self.OK_RESPONSE_CODE:
            print("Warning: response status code {}".format(response.status_code))
            header = "; ".join([str(key) + " : " + str(value) for key, value in response.headers.items()])
            print("Response header: \n{}".format(header))
            return data, "ERROR"
        root = Etree.fromstring(response.text)
        for i, spellResult in enumerate(root):
            if len(spellResult):
                for error in spellResult.findall("error"):
                    word = error.find("word").text
                    correction = error.find("s").text
                    data.iloc[i].text = data.iloc[i].text.replace(word, correction)
        return data, "OK"


class Stemmer:

    def __init__(self):
        self.stemmer = Mystem(grammar_info=False)
        self.path_postfix = "_stemmer_corrected.tsv"

    def __str__(self):
        return "Stemmer"

    def transform(self, data, output_path):
        data["text"] = data["text"].apply(lambda x: "".join(self.stemmer.lemmatize(x)[:-1]))
        data.to_csv(output_path, sep="\t")
        return data


class StopWords:

    def __str__(self):
        return "Stop words"

    def __init__(self, stop_words=nltk_stopwords.words("russian")):
        self.path_postfix = "_stopwords.tsv"
        self.tokenizer = CountVectorizer(lowercase=False, stop_words=stop_words, token_pattern=r"\w+").build_analyzer()

    def transform(self, data, output_path):
        data["text"] = data["text"].apply(lambda x: " ".join(self.tokenizer(x)))
        data.to_csv(output_path, sep="\t")
        return data


class Tokenizer:
    def __str__(self):
        return "Tokenizer"

    def __init__(self, mode="word"):
        if mode == "char_3":
            params = {"lowercase": False, "ngram_range": (3, 3), "analyzer": "char_wb", "strip_accents": "unicode"}
            self.path_postfix = "_char_3_tokenized.pkl"
        elif mode == "word":
            params = {"lowercase": False, "token_pattern": r"\w+", "strip_accents": "unicode"}
            self.path_postfix = "_word_tokenized.pkl"
        elif mode == "word_2":
            params = {"lowercase": False, "ngram_range": (2, 2), "analyzer": "word", "strip_accents": "unicode"}
            self.path_postfix = "_word_2_tokenized.pkl"
        elif mode == "char_4":
            params = {"lowercase": False, "ngram_range": (4, 4), "analyzer": "char_wb", "strip_accents": "unicode"}
            self.path_postfix = "_char_4_tokenized.pkl"
        else:
            print("Error: wrong mode")
            return
        self.tokenizer = CountVectorizer(**params).build_analyzer()

    def transform(self, data, output_path):
        data["text"] = data["text"].apply(self.tokenizer)
        data.to_pickle(output_path)
        return data


class Preprocessor:

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def set_pipeline(self, pipeline):
        self.pipeline = pipeline

    def transform(self, data, path_prefix):
        print("Start transformation pipeline: {}".format(" -> ".join([str(x) for x in self.pipeline])))
        for transformer in self.pipeline:
            print("{} correction started...".format(transformer))
            output_path = path_prefix / (path_prefix.stem + transformer.path_postfix)
            t0 = time()
            transformer.transform(data, output_path)
            t1 = time()
            print("...finished in {:4.2f}s. Result written to '{}'".format(t1 - t0, output_path))


def process_queries():
    path_prefix = Path("../data/queries")
    # pipeline = [Lower(), Speller(), Stemmer(), Tokenizer("word")]
    # pipeline = [Tokenizer(mode="char_4"), ]
    # preprocessor = Preprocessor(pipeline)
    # df = pd.read_csv("../data/queries/queries_stemmer_corrected.tsv", sep="\t", index_col=0)
    # preprocessor.transform(df, path_prefix)
    #
    pipeline = [FastText("nn")]
    preprocessor = Preprocessor(pipeline)
    df = pd.read_csv("../data/queries/queries_spell_corrected.tsv", sep="\t", index_col=0)
    preprocessor.transform(df, path_prefix)


def process_titles():
    path_prefix = Path("../data/titles")
    # pipeline = [Cutter(), Lower(), Stemmer(), Tokenizer("word_2")]
    pipeline = [Tokenizer("word_2")]
    preprocessor = Preprocessor(pipeline=pipeline)

    df = pd.read_csv("../data/titles/titles_stemmer_corrected.tsv", sep="\t", index_col=0).fillna("")
    preprocessor.transform(df, path_prefix)

    # pipeline = [FastText(word_embeddings=False, sentence_embeddings=True)]
    # preprocessor = Preprocessor(pipeline)
    # df = pd.read_csv("../data/titles_lowered.tsv", sep="\t", index_col=0).fillna("")
    # preprocessor.transform(df, path_prefix)


def main():
    # process_queries()
    process_titles()


if __name__ == "__main__":
    main()
