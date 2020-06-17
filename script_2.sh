#!/bin/bash

# Data download
DIR="data/"
mkdir -p $DIR
echo "Loading combined data from Google Drive..."

FILEID="1L1KcagrfTwJl-k4uUoq86EBakruopN30"
FILENAME=$DIR"/script_2_data.tar.gz"
wget --load-cookies /tmp/cookies.txt "https://docs.google.com/uc?export=download&confirm=$(wget --quiet --save-cookies /tmp/cookies.txt --keep-session-cookies --no-check-certificate 'https://docs.google.com/uc?export=download&id='$FILEID -O- |
sed -rn 's/.*confirm=([0-9A-Za-z_]+).*/\1\n/p')&id="$FILEID -O $FILENAME &&
rm -rf /tmp/cookies.txt
echo "Loading combined data from Google Drive...OK"

echo "Unpacking data archive..."
tar -zxvf $FILENAME --directory $DIR
echo "Unpacking data archive...OK"

echo "Remove archive file..."
rm --verbose $FILENAME
echo "Remove archive file...OK"


echo "Data loading finished"


# Run python text preprocessing code
echo "Run python text preprocessing code..."
SRC_DIR="src/python"
python3 $SRC_DIR"/text_preprocessing.py"