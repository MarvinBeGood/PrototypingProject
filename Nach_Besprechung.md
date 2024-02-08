## Nach Bespechung

### Positive Argumente für die Umstellung auf Delta Lake oder Hudi:
- Die Frameworks können mit kleinen Dateien besser umgehen 
- SCD2 kann durch die Frameworks implementiert werden 
- funktionen werden durch die Frameworks für historisierung bereitgestellt   
- Datenschutz Themen können besser behandelt werden

### Negative Argumente für die Umstellung auf Delta Lake oder Hudi:

- Die Funktion time travel von den Frameworks ist nicht das gleich wie bei das Watermarking wie vom Watermark service 
- Der schema check ist vielleicht nicht das gleiche wie in spark sdk und würde uns keinen Aufwand sparen
- Im moment keine Kapazität im Team, weil andere Themen schon geplant sind
- Die Integration könnte einen großer schmerz sein, weil wir technisch Durchdenken müssen wie wir die Frameworks für die bereits bestehende Implementierungen integrieren
- (Fachlichen Gründe) Anforderungen fehlen noch für die Umsetzungen 


### Ideen zur verprobung von Delta Lake oder Hudi:
- Wir müssen im Datenmodell bzw. bei Historisierung Thema verproben


### Allgemeines
- Datenschutz Anforderungen werden den vom Fachbereich nicht weiter kommuniziert 