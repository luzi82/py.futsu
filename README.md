# Futsu

Futsu = Japanese 普通 , means general, ordinary, usual.

You can see this lib is like Apache Commons, but in python.

https://github.com/luzi82/py.futsu


## Cheat sheet

```
gcloud config configurations create futsu
gcloud config set project futsu
export GOOGLE_APPLICATION_CREDENTIALS=${PWD}/key/futsu-4bc2f6db1c50.json

virtualenv --python python3 venv
source venv/bin/activate

pip install --upgrade setuptools wheel nose twine keyring google-cloud-storage boto3

keyring set https://upload.pypi.org/legacy/ luzi82
keyring set https://test.pypi.org/legacy/ luzi82

rm -rf dist
python3 setup.py test
python3 setup.py sdist bdist_wheel

python3 -m twine upload -u luzi82 --repository-url https://test.pypi.org/legacy/ dist/*

deactivate

mkdir -p tmp;cd tmp
rm -rf venv_test
virtualenv --python python3 venv_test
source venv_test/bin/activate
pip install --index-url https://test.pypi.org/simple/ --no-deps futsu
python3 -c "import futsu;print(futsu.name)"
deactivate
cd ..

source venv/bin/activate
python3 -m twine upload -u luzi82 dist/*
deactivate

# short output

   rm -rf dist \
&& python3 setup.py test \
&& python3 setup.py sdist bdist_wheel \
&& python3 -m twine upload -u luzi82 dist/*

```
