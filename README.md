# Futsu

Futsu = Japanese 普通 , means general, ordinary, usual.

You can see this lib is like Apache Commons, but in python.

https://github.com/luzi82/py.futsu


## Cheat sheet

```
virtualenv --python python3 venv
source venv/bin/activate

pip install --upgrade setuptools wheel nose twine
python3 setup.py test
python3 setup.py sdist bdist_wheel

python3 -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*

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
python3 -m twine upload dist/*
deactivate
```
