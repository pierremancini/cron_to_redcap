Dépot figé d'un projet réalisé à l'instut Bergonié entre 2016 et 2018.

Pour suivre les évolutions du projet depuis 2018 aller sur le dépot principal :
https://bitbucket.org/bergomultipli/generate_mtb_reports/src/default/

# cron to redcap


## Installation

```
hg clone ssh://hg@bitbucket.org/bergomultipli/cron_to_redcap
```

## Configurations

Après installation il faut préciser des chemins pour path_to_log, path_to_data, remote_crf_file,


Nb: Quand les scripts sont appelés par un crontab il est important de donner les path en chemin
absolu car dans le cas contraire les chemins seront relatif au crontab et pas au script appelé par 
le crontab.

## Usage

Les scripts cron_crf.py, cron_cng.py et redcap_to_crf.py prennent en argument les chemins des
deux fichiers de configuration config.yml et secret_config.yml

```
python3 cron_crf.py -c /path/to/config.yml -s /path/to/secret_config.yml
```

Nb: Quand les scripts sont appelés par un crontab il est important de donner les path en chemin
absolu car dans le cas contraire les chemins seront relatif au crontab et pas au script appelé par 
le crontab.


## Tests

### Tests unitaires

Des tests unitaire on été développés sur deux fonctions clés du projet.
Pour lancer les tests:

- cd cron_to_redcap
- pytest -vv

La couvertures des tests untaires étant très faible il faut également réaliser des tests
end-to-end après avoir modifié le code du projet.

### Tests end-to-ends

#### cron_crf.py

Utiliser l'argument --dev permet de lire un autre fichier que le fichier du serveur ennov. 
Cf. data/crf_extraction/mock_MULTIPLI_Sequencing_barcode.tsv

Cela évite les transferts ftp inutiles durant les phases de développement.

```
python3 cron_crf.py --dev
```

#### cron_cng.py

Utiliser l'arguement --mock pour utiliser un mock des données de la page web du cng.

La récupération des données depuis le page web et l'étape la plus lente du script.
Avec --mock on ne lit pas la page web mais un fichier contenant des données similaires.
Cela réduit le temps d'excution du fichier.

```
python3 cron_crf.py --mock
```

Nb: Lorsqu'on execute cron_cng.py en dehors de sont appel hebdomadaire par le crontab de k2so il faut 
expliciter les sets à ignorer (ignored_set) et les sets contenant les information souhaitées (mandatory_set).
Par défaut tout les sets cloturés seront ignorés. Un set est considéré comme cloturé quand
il dans le champ "Set" d'au moins un record.