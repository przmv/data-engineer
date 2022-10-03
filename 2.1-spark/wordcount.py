from pyspark import SparkConf, SparkContext

# Настройки Spark
conf = SparkConf()
conf.setAppName("Word count")
sc = SparkContext(conf=conf)

# Читаем строки из текстового файла
rdd = sc.textFile("war-and-peace.txt")

# Используем flatMap(), чтобы на каждое входное значение
# возвращать несколько элементов, разделённых пробельными символами.
# Так же, все слова переводятся в нижний регистр
# (записываются строчными буквами).
rdd = rdd.flatMap(lambda x: x.lower().split())

# Создаём кортежи и добавляем 1 к каждому слову.
rdd = rdd.map(lambda x: (x, 1))

# Производим агрегацию: суммируем все значения для каждого уникального ключа.
rdd = rdd.reduceByKey(lambda x, y: x + y)

# Меняем ключ и значение местами, сортируем по ключу в обратном порядке
# и берём десятку самых используемых слов.
rdd = rdd.map(lambda x: (x[1], x[0])).sortByKey(False).take(10)

# Вывод на экран
for (count, word) in rdd:
    print("%s: %i" % (word, count))

