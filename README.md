# Distributed Automatic Domain-Specific Multi-Word Expression Recognition using Spark Ecosystem

## Article:

Ciprian-Octavian Truică, Elena-Simona Apostol. *Distributed Automatic Domain-Specific Multi-Word Expression Recognition using Spark Ecosys*. Arxiv, 2023. Link: [https://arxiv.org/abs/2302.12190](https://arxiv.org/abs/2302.12190) 

## Prerequisites

- Apache Spark Environemnt
- MongoDB
- Python

## Code 

Packages needed:
- pymongo
- pyspark
- spacy
- nltk

Files:
- ``utils.py`` - script with configuration parameters (change to reflect your configuration)
- ``TextPreprocessing.py`` - script for text preprocessing
- ``C_Value.py`` - script for computing the C-Value
- ``NC_Value.py`` - script for computing the NC-Value
- ``LIDF_Value.py`` - script for computing the LIDF-Value

## Utilization

Run in the following order:

``python3 TextPreprocessing.py DATASET_FOLDER``

``python3 C_Value.py``

``python3 NC_Value.py``

``python3 LIDF_Value.py``
