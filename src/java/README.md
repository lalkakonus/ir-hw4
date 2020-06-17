# Hadoop Map-Reduce

В папке *src/main/java* находится код для подсчёта кликовых статистик, агрегации подсчитанных статистик и для извлечения заголовков из документов.

Для компиляции задач запустите:

```
./gradlew jar
```

---

### Подсчёт кликовых статистик

Map-Reduce задача находится в файле *src/main/java/StatisticsProcessJob.java*
Перед запуском задачи необходимо добавить файлы конфигурации в hadoop. Архив с файлами находится на [Google Drive](https://drive.google.com/file/d/1nQko3WjMaZhpCxnMTROlY0nVvb8wym68/view?usp=sharing).

Запуск задачи на map-reduce:

```
hadoop jar java.jar StatisticProcessJob <path/to/unpacked/data> <path/to/output>
```

---

### Аггрегация кликовых статистик

Map-Reduce задача находится в файле *src/main/java/DatasetFormatJob.java*

Запуск задачи на map-reduce:

```
hadoop jar java.jar DatasetFormatJob <path/to/data> <path/to/output>
```

---

### Извлечение заголовков документов

Map-Reduce задача находится в файле *src/main/java/TitleExtractorJob.java*

Запуск задачи на map-reduce:

```
hadoop jar java.jar TitleExtractorJob <path/to/unpacked/data> <path/to/output>
```
