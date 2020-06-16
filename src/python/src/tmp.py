import xml.etree.ElementTree as Etree
import requests
from tqdm import tqdm
import pandas as pd


class Synonims:

    def __init__(self):
        self.url_base = "https://dictionary.yandex.net/api/v1/dicservice/lookup?key={}&lang=ru-en&text={}"
        self.api_key = "dict.1.1.20200613T220756Z.7e951c64a485c9d4.bfbfd64238ba93df1bbf312634c0d252dbe537f5"
        self.OK_RESPONSE_CODE = 200

    def _send_request(self, word):
        url = self.url_base.format(self.api_key, word)
        try:
            response = requests.get(url)
        except Exception as err:
            print(err)
            return []
        if response.status_code != self.OK_RESPONSE_CODE:
            print("Warning: response status code {}".format(response.status_code))
            header = "; ".join([str(key) + " : " + str(value) for key, value in response.headers.items()])
            print("Response header: \n{}".format(header))
            return []
        root = Etree.fromstring(response.text)
        result = []
        if len(root) > 1:
            for i, spellResult in enumerate(root[1].findall("*.mean")):
                result.append(spellResult[0].text)
            return result

    def get_synonims(self, corpus):
        dict = {}
        for word in tqdm(corpus):
            syns = self._send_request(word)
            if syns:
                dict[word] = syns
        return dict

queries_df = pd.read_csv("../data/queries/queries_spell_corrected.tsv", sep="\t", index_col=0)
corpus = set()
for query in queries_df["text"]:
  corpus.update(query.split())

# syn = Synonims().get_synonims(["умный", "красивый"])
syn = Synonims().get_synonims(corpus)
data = []
for key, value in syn.items():
    data += [(key, x) for x in value]
df = pd.DataFrame(data, columns=["word_a", "word_b"])
df.to_csv("../data/queries/similar_words.tsv", sep="\t", index=False)