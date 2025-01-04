import pandas as pd
import matplotlib.pyplot as plt
import time
import os

# Проверяем, существует ли файл и не пуст ли он
if os.path.isfile('./logs/metric_log.csv') and os.path.getsize('./logs/metric_log.csv') > 0:
    # Чтение данных из metric_log.csv
    df = pd.read_csv('./logs/metric_log.csv', sep=';')

    # Выводим названия столбцов для диагностики
    print("Столбцы в DataFrame:", df.columns.tolist())

    # Проверяем, есть ли нужный столбец
    if 'absolute_error' in df.columns:
        if not df.empty:
            # Построение гистограммы абсолютных ошибок
            plt.figure(figsize=(10, 6))
            plt.hist(df['absolute_error'], bins=30, color='blue', alpha=0.7)
            plt.title('Distribution of Absolute Errors')
            plt.xlabel('Absolute Error')
            plt.ylabel('Frequency')

            # Сохранение графика в файл
            plt.savefig('./logs/error_distribution.png')
            plt.close()
            print("Гистограмма обновлена.")
        else:
            print("Данных для построения гистограммы пока нет.")
    else:
        print("Столбец 'absolute_error' не найден в данных.")
else:
    print("Файл metric_log.csv не найден или пуст.")

# Задержка перед следующим обновлением
time.sleep(10)
