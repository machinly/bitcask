# About Bitcask
Bitcask is Key-Value Database engine, which used Log-Structured and Hash Table Index.
The [paper](https://riak.com/assets/bitcask-intro.pdf) post by riak will show you more detail about bitcask.

# About This Project
This Project is bitcask implementation in Golang. It's just for understand bitcask, **not** for production.

# TL;DR
```shell
make build
./bin/bitcask
>>> put a b
>>> list
>>> get a
>>> exit
```
