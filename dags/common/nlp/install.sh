python3 -m pip install -r requirements.txt
fileid="16Neexi-QyX-WettcN6Oc6mNNuK4NlsIr"
filename="roberta.zip"
curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=${fileid}"
curl -Lb ./cookie "https://drive.google.com/uc?export=download&confirm=`awk '/download/ {print $NF}' ./cookie`&id=${fileid}" -o ${filename}

fileid="1TQpXv7M22A4wHro7GWX3bzFqKEy46ypG"
filename="model_save.zip"
curl -c ./cookie -s -L "https://drive.google.com/uc?export=download&id=${fileid}" 
curl -Lb ./cookie "https://drive.google.com/uc?export=download&confirm=`awk '/download/ {print $NF}' ./cookie`&id=${fileid}" -o ${filename}

unzip model_save
rm model_save.zip

unzip roberta
rm roberta.zip