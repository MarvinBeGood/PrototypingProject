## Erkenntnisse 

## Delta Lake


### Negatives
- polars benötigt eigene DynamoDB Tabelle um gleichzeitiges schreiben zu verhindern
- spark hat sein eigenen Mechanismus um gleichzeitiges schreiben zu verhindern
- bei polars werden nicht alle protokoll versionen unterstützt was dazu führt das nicht alle funktionen genutzt werden können
- Metadaten werden getrennt von den Daten gelöscht werden


### Positives
- arbeitet mit Data Skipping um weniger lese operationen zu haben 
- funktioniert mit spark und polars
- integration zu glue data catalog: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html
- Alle Aktionen die auf der Tabelle ausgeführt wurden werden in der History gespeichert

## Hudi


### Negatives
- funktioniert nur mit spark
- keine Python Module
- Interface wird nur mit Dicts angesprochen 
- Metadaten werden abgespeichert sind aber sind nicht selbst erklärend wie bei Deltalake

### Positives
- hat mehr Features als Delta Lake wie z.B Change Data Capture, Data Clustering, Indexing 
- integration zu glue data catalog: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-hudi.html
- Nullable Felder werden strickt geprüft 