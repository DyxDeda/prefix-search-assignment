import json
import re
import time
import xml.etree.ElementTree as ET
from elasticsearch import Elasticsearch, helpers
import pandas as pd

def normalize(text):
    if text is None:
        return ''
    text = text.lower()
    text = re.sub(r'[^а-яa-z0-9 ]', ' ', text)
    text = ' '.join(text.split())
    
    if any(c in 'qwertyuiopasdfghjklzxcvbnm' for c in text.lower()):
        translit_table = str.maketrans(
            "qwertyuiop[]asdfghjkl;'zxcvbnm,.",
            "йцукенгшщзхъфывапролджэячсмитьбю"
        )
        text = text.translate(translit_table)
    
    return text

def fix_common_typos(query):
    fixes = {
        'кар тофель': 'картофель',
        'греч не': 'гречневая', 
        'хозяйст мыло': 'хозяйственное мыло',
        'diap night': 'diapers night',
        'санпел': 'san pellegrino',
        'prosc ros': 'prosecco rose'
    }
    return fixes.get(query, query)

print("⏳ Waiting for Elasticsearch to start...")

max_retries = 30
es = None

for i in range(max_retries):
    try:
        es = Elasticsearch(['http://es:9200'], request_timeout=30)
        if es.ping():
            print("✅ Connected to Elasticsearch")
            break
        time.sleep(5)
    except Exception:
        time.sleep(5)

if not es or not es.ping():
    raise ConnectionError("❌ Cannot connect to Elasticsearch")

if es.indices.exists(index='products'):
    es.indices.delete(index='products')

mapping = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0,
        'analysis': {
            'analyzer': {
                'edge_ngram_analyzer': {
                    'type': 'custom',
                    'tokenizer': 'standard',
                    'filter': ['lowercase', 'edge_ngram']
                }
            },
            'filter': {
                'edge_ngram': {
                    'type': 'edge_ngram',
                    'min_gram': 2,
                    'max_gram': 10
                }
            }
        }
    },
    'mappings': {
        'properties': {
            'name': {
                'type': 'text',
                'analyzer': 'edge_ngram_analyzer',
                'search_analyzer': 'standard'
            },
            'description': {'type': 'text'},
            'category': {'type': 'text'},
            'brand': {'type': 'text'},
            'keywords': {'type': 'text'},
            'weight': {'type': 'text'},
            'package_size': {'type': 'text'},
            'price': {'type': 'float'}
        }
    }
}

es.indices.create(index='products', body=mapping)

tree = ET.parse('data/catalog_products.xml')
root = tree.getroot()
products = root.findall('.//product')

actions = []
for product in products:
    try:
        product_id = product.get('id')
        if not product_id:
            continue

        doc = {
            '_index': 'products',
            '_id': product_id,
            'name': normalize(product.findtext('name')),
            'description': normalize(product.findtext('description')),
            'category': normalize(product.findtext('category')),
            'brand': normalize(product.findtext('brand')),
            'keywords': normalize(product.findtext('keywords')),
            'weight': normalize(product.findtext('weight')),
            'package_size': normalize(product.findtext('package_size')),
            'price': float(product.findtext('price') or 0.0)
        }
        actions.append(doc)
    except Exception:
        continue

if actions:
    helpers.bulk(es, actions, stats_only=True)
    es.indices.refresh(index='products')

def search_prefix(prefix):
    fixed_query = fix_common_typos(prefix)
    norm_prefix = normalize(fixed_query)
    
    query = {
        'query': {
            'bool': {
                'should': [
                    {'match': {'name': {'query': norm_prefix, 'boost': 3.0, 'fuzziness': 'AUTO'}}},
                    {'match': {'category': {'query': norm_prefix, 'boost': 2.0, 'fuzziness': 'AUTO'}}},
                    {'match': {'brand': {'query': norm_prefix, 'boost': 1.5}}},
                    {'match': {'keywords': {'query': norm_prefix, 'boost': 1.2}}},
                    {'match': {'description': norm_prefix}}
                ]
            }
        },
        'size': 10
    }
    
    try:
        res = es.search(index='products', body=query)
        return [{
            'id': hit['_id'],
            'name': hit['_source'].get('name', ''),
            'category': hit['_source'].get('category', ''),
            'score': float(hit['_score'])
        } for hit in res['hits']['hits']]
    except Exception:
        return []

queries = pd.read_csv('data/prefix_queries.csv')
results = {}
successful_searches = 0

for _, row in queries.iterrows():
    prefix = row['query']
    search_results = search_prefix(prefix)
    results[prefix] = search_results
    if search_results:
        successful_searches += 1

coverage = (successful_searches / len(queries)) * 100

with open('/app/data/results.json', 'w', encoding='utf-8') as f:
    json.dump(results, f, ensure_ascii=False, indent=2)

stats = {
    'total_queries': len(queries),
    'successful_searches': successful_searches,
    'coverage_percentage': round(coverage, 1)
}

with open('/app/data/stats.json', 'w', encoding='utf-8') as f:
    json.dump(stats, f, ensure_ascii=False, indent=2)

print(f"✅ Coverage: {coverage:.1f}%")