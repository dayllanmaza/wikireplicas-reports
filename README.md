# wikireplicas-reports
The purpose of this repo is to keep a quick and dirty collection of "scripts" that query the replicas databases and generate (or not) any type of raw data for future analysis.

# What you need
- Access to toolforge <https://wikitech.wikimedia.org/wiki/Help:Access#Accessing_Toolforge_instances>
- A working ssh tunnel to the replicas binded to 0.0.0.0:3306
```
ssh -N yourusername@tools-dev.wmflabs.org -L 0.0.0.0:3306:enwiki.analytics.db.svc.eqiad.wmflabs:3306
```
- pipenv <https://pipenv.readthedocs.io/en/latest/install/#installing-pipenv>
- Your replica.my.cnf
```
scp yourusername@tools-dev.wmflabs.org:replica.my.cnf ./
```

# Install
pipenv install

# Usage
```
pipenv run python main.py [generator_name]
```
if generator_name is not specified then it tries to run everything instead

# Adding your own script?
If you want to create a new script to generate data you can do so by adding a file to the generators folder and add it to the generators module __all__.
