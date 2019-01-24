import pandas as pd
from collections import Counter

df = pd.read_csv('seq.csv', sep='|')
df.sepsis_seq = df.sepsis_seq.str.split(',')
patients = df.subject_id.tolist()
seq = df.sepsis_seq.tolist()

def window(iterable, size=2):
    i = iter(iterable)
    win = []
    for e in range(0, size):
        win.append(next(i))
    yield win
    for e in i:
        win = win[1:] + [e]
        yield win

def change_name(name):
    long_name = []
    if "e" in name:
        long_name.append("Explicit Sepsis")
    if "i" in name:
        long_name.append('Infection')
    if "o" in name:
        long_name.append("organ_dysfunction")
    if "m" in name:
        long_name.append("mech_vent")
    return ','.join(long_name)

tuples = []
links = []
nodes = set()
first_nodes = []

for s in seq:
    first_nodes.append(change_name(s[0]))
    for source, target in window(s):
        source = change_name(source)
        target = change_name(target)
        if source != target:
            tuples.append((source, target))

for k, v in Counter(tuples).items():
    source = k[0]
    target = k[1]
    nodes.add(source)
    nodes.add(target)
    links.append(
        dict(source=source,
             target=target,
             value=v))
    
nodes = [dict(name=n) for n in list(nodes)]

print(nodes)
print(links)